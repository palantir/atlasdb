/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.timelock.paxos;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.streams.KeyedStream;
import com.palantir.leader.PingableLeader;
import com.palantir.paxos.ImmutableLeaderPingerContext;
import com.palantir.paxos.LeaderPingerContext;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.timelock.paxos.TimelockPaxosAcceptorRpcClient;
import com.palantir.timelock.paxos.TimelockPaxosLearnerRpcClient;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import javax.ws.rs.Path;
import org.immutables.value.Value;

@Value.Immutable
public abstract class PaxosRemoteClients {

    @Value.Parameter
    public abstract PaxosResourcesFactory.TimelockPaxosInstallationContext context();

    @Value.Parameter
    public abstract MetricsManager metrics();

    @Value.Derived
    Map<String, ExecutorService> dedicatedExecutors() {
        List<String> remoteUris = context().remoteUris();
        int executorIndex = 0;

        ImmutableMap.Builder<String, ExecutorService> builder = ImmutableMap.builder();
        for (String remoteUri : remoteUris) {
            builder.put(remoteUri, TimeLockPaxosExecutors.createBoundedExecutor(
                    metrics().getRegistry(),
                    "paxos-remote-clients-dedicated-executors",
                    executorIndex));
            executorIndex++;
        }
        return builder.build();
    }

    @Value.Derived
    public List<TimelockPaxosAcceptorRpcClient> nonBatchTimestampAcceptor() {
        return createInstrumentedRemoteProxyList(TimelockPaxosAcceptorRpcClient.class, true);
    }

    @Value.Derived
    public List<TimelockPaxosLearnerRpcClient> nonBatchTimestampLearner() {
        return createInstrumentedRemoteProxyList(TimelockPaxosLearnerRpcClient.class, true);
    }

    @Value.Derived
    public List<WithDedicatedExecutor<TimelockSingleLeaderPaxosAcceptorRpcClient>>
            singleLeaderAcceptorsWithExecutors() {
        // Retries should be performed at a higher level, in AwaitingLeadershipProxy.
        return createInstrumentedRemoteProxiesAndAssignDedicatedExecutors(
                TimelockSingleLeaderPaxosAcceptorRpcClient.class, false);
    }

    @Value.Derived
    public List<TimelockSingleLeaderPaxosLearnerRpcClient> singleLeaderLearner() {
        return createInstrumentedRemoteProxyList(TimelockSingleLeaderPaxosLearnerRpcClient.class, true);
    }

    @Value.Derived
    public List<WithDedicatedExecutor<BatchPaxosAcceptorRpcClient>> batchAcceptorsWithExecutors() {
        return createInstrumentedRemoteProxiesAndAssignDedicatedExecutors(
                BatchPaxosAcceptorRpcClient.class, false);
    }

    @Value.Derived
    public List<BatchPaxosLearnerRpcClient> batchLearner() {
        return createInstrumentedRemoteProxyList(BatchPaxosLearnerRpcClient.class, true);
    }

    @Value.Derived
    public List<PingableLeader> nonBatchPingableLeaders() {
        return nonBatchPingableLeadersWithContext().stream()
                .map(LeaderPingerContext::pinger)
                .collect(Collectors.toList());
    }

    @Value.Derived
    public List<BatchPingableLeader> batchPingableLeaders() {
        return batchPingableLeadersWithContext().stream()
                .map(LeaderPingerContext::pinger)
                .collect(Collectors.toList());
    }

    @Value.Derived
    public List<LeaderPingerContext<PingableLeader>> nonBatchPingableLeadersWithContext() {
        return leaderPingerContext(PingableLeader.class);
    }

    @Value.Derived
    public List<LeaderPingerContext<BatchPingableLeader>> batchPingableLeadersWithContext() {
        return leaderPingerContext(BatchPingableLeader.class);
    }

    private <T> List<LeaderPingerContext<T>> leaderPingerContext(Class<T> clazz) {
        return createInstrumentedRemoteProxies(clazz, false)
                .mapKeys(PaxosRemoteClients::convertAddressToHostAndPort)
                .entries()
                .<LeaderPingerContext<T>>map(entry -> ImmutableLeaderPingerContext.of(entry.getValue(), entry.getKey()))
                .collect(Collectors.toList());
    }

    private <T> List<T> createInstrumentedRemoteProxyList(Class<T> clazz, boolean shouldRetry) {
        return createInstrumentedRemoteProxies(clazz, shouldRetry).values().collect(Collectors.toList());
    }

    private <T> List<WithDedicatedExecutor<T>> createInstrumentedRemoteProxiesAndAssignDedicatedExecutors(
            Class<T> clazz,
            boolean shouldRetry) {
        return createInstrumentedRemoteProxies(clazz, shouldRetry)
                .mapKeys(uri -> dedicatedExecutors().get(uri))
                .entries()
                .map(entry -> WithDedicatedExecutor.<T>of(entry.getValue(), entry.getKey()))
                .collect(Collectors.toList());
    }

    private <T> KeyedStream<String, T> createInstrumentedRemoteProxies(Class<T> clazz, boolean shouldRetry) {
        return KeyedStream.of(context().remoteUris())
                .map(uri -> context().dialogueServiceProvider()
                        .createSingleNodeInstrumentedProxy(uri, clazz, shouldRetry))
                .map((host, proxy) -> AtlasDbMetrics.instrumentWithTaggedMetrics(
                        metrics().getTaggedRegistry(),
                        clazz,
                        proxy,
                        any -> ImmutableMap.of(AtlasDbMetricNames.TAG_REMOTE_HOST, host)));
    }

    private static HostAndPort convertAddressToHostAndPort(String url) {
        URI uri = URI.create(url);
        return HostAndPort.fromParts(uri.getHost(), uri.getPort());
    }

    @Path("/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE
            + "/" + PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE)
    public interface TimelockSingleLeaderPaxosLearnerRpcClient extends PaxosLearner {

    }

    @Path("/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE
            + "/" + PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE)
    public interface TimelockSingleLeaderPaxosAcceptorRpcClient extends PaxosAcceptor {

    }
}
