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
import com.palantir.common.concurrent.CheckedRejectionExecutorService;
import com.palantir.common.streams.KeyedStream;
import com.palantir.leader.PingableLeader;
import com.palantir.paxos.ImmutableLeaderPingerContext;
import com.palantir.paxos.LeaderPingerContext;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.timelock.corruption.TimeLockCorruptionNotifier;
import com.palantir.timelock.history.TimeLockPaxosHistoryProvider;
import com.palantir.timelock.paxos.TimelockPaxosAcceptorRpcClient;
import com.palantir.timelock.paxos.TimelockPaxosLearnerRpcClient;
import java.net.URI;
import java.util.List;
import java.util.Map;
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
    Map<String, CheckedRejectionExecutorService> paxosExecutors() {
        List<String> remoteUris = context().remoteUris();
        int executorIndex = 0;

        ImmutableMap.Builder<String, CheckedRejectionExecutorService> builder = ImmutableMap.builder();
        for (String remoteUri : remoteUris) {
            builder.put(
                    remoteUri,
                    TimeLockPaxosExecutors.createBoundedExecutor(
                            TimeLockPaxosExecutors.MAXIMUM_POOL_SIZE,
                            "paxos-remote-clients-paxos-executors",
                            executorIndex));
            executorIndex++;
        }
        return builder.build();
    }

    @Value.Derived
    Map<String, CheckedRejectionExecutorService> pingExecutors() {
        List<String> remoteUris = context().remoteUris();
        int executorIndex = 0;

        ImmutableMap.Builder<String, CheckedRejectionExecutorService> builder = ImmutableMap.builder();
        for (String remoteUri : remoteUris) {
            builder.put(
                    remoteUri,
                    TimeLockPaxosExecutors.createBoundedExecutor(
                            8, // 1 is probably enough, but be defensive for now.
                            "paxos-remote-clients-ping-executors",
                            executorIndex));
            executorIndex++;
        }
        return builder.build();
    }

    @Value.Derived
    public List<WithDedicatedExecutor<TimelockPaxosAcceptorRpcClient>> nonBatchTimestampAcceptor() {
        return createInstrumentedRemoteProxiesAndAssignDedicatedPaxosExecutors(
                TimelockPaxosAcceptorRpcClient.class, true);
    }

    @Value.Derived
    public List<WithDedicatedExecutor<TimelockPaxosLearnerRpcClient>> nonBatchTimestampLearner() {
        return createInstrumentedRemoteProxiesAndAssignDedicatedPaxosExecutors(
                TimelockPaxosLearnerRpcClient.class, true);
    }

    @Value.Derived
    public List<WithDedicatedExecutor<TimelockSingleLeaderPaxosAcceptorRpcClient>>
            singleLeaderAcceptorsWithExecutors() {
        // Retries should be performed at a higher level, in AwaitingLeadershipProxy.
        return createInstrumentedRemoteProxiesAndAssignDedicatedPaxosExecutors(
                TimelockSingleLeaderPaxosAcceptorRpcClient.class, false);
    }

    @Value.Derived
    public List<WithDedicatedExecutor<TimelockSingleLeaderPaxosLearnerRpcClient>> singleLeaderLearner() {
        return createInstrumentedRemoteProxiesAndAssignDedicatedPaxosExecutors(
                TimelockSingleLeaderPaxosLearnerRpcClient.class, true);
    }

    @Value.Derived
    public List<WithDedicatedExecutor<BatchPaxosAcceptorRpcClient>> batchAcceptorsWithExecutors() {
        return createInstrumentedRemoteProxiesAndAssignDedicatedPaxosExecutors(
                BatchPaxosAcceptorRpcClient.class, false);
    }

    @Value.Derived
    public List<WithDedicatedExecutor<BatchPaxosLearnerRpcClient>> batchLearner() {
        return createInstrumentedRemoteProxiesAndAssignDedicatedPaxosExecutors(BatchPaxosLearnerRpcClient.class, true);
    }

    @Value.Derived
    public List<PingableLeader> nonBatchPingableLeaders() {
        return nonBatchPingableLeadersWithContext().stream()
                .map(WithDedicatedExecutor::service)
                .map(LeaderPingerContext::pinger)
                .collect(Collectors.toList());
    }

    @Value.Derived
    public List<BatchPingableLeader> batchPingableLeaders() {
        return batchPingableLeadersWithContext().stream()
                .map(WithDedicatedExecutor::service)
                .map(LeaderPingerContext::pinger)
                .collect(Collectors.toList());
    }

    @Value.Derived
    public List<WithDedicatedExecutor<LeaderPingerContext<PingableLeader>>> nonBatchPingableLeadersWithContext() {
        return leaderPingerContext(PingableLeader.class);
    }

    @Value.Derived
    public List<WithDedicatedExecutor<LeaderPingerContext<BatchPingableLeader>>> batchPingableLeadersWithContext() {
        return leaderPingerContext(BatchPingableLeader.class);
    }

    @Value.Derived
    public List<TimeLockCorruptionNotifier> getRemoteCorruptionNotifiers() {
        return createInstrumentedRemoteProxies(TimeLockCorruptionNotifier.class, true)
                .values()
                .collect(Collectors.toList());
    }

    @Value.Derived
    public List<TimeLockPaxosHistoryProvider> getRemoteHistoryProviders() {
        return createInstrumentedRemoteProxies(TimeLockPaxosHistoryProvider.class, true)
                .values()
                .collect(Collectors.toList());
    }

    private <T> List<WithDedicatedExecutor<LeaderPingerContext<T>>> leaderPingerContext(Class<T> clazz) {
        return createInstrumentedRemoteProxies(clazz, false)
                .<WithDedicatedExecutor<LeaderPingerContext<T>>>map((uri, remote) -> WithDedicatedExecutor.of(
                        ImmutableLeaderPingerContext.of(remote, PaxosRemoteClients.convertAddressToHostAndPort(uri)),
                        pingExecutors().get(uri)))
                .values()
                .collect(Collectors.toList());
    }

    private <T> List<WithDedicatedExecutor<T>> createInstrumentedRemoteProxiesAndAssignDedicatedPaxosExecutors(
            Class<T> clazz, boolean shouldRetry) {
        return assignDedicatedRemotePaxosExecutors(createInstrumentedRemoteProxies(clazz, shouldRetry));
    }

    private <T> List<WithDedicatedExecutor<T>> assignDedicatedRemotePaxosExecutors(KeyedStream<String, T> remotes) {
        return remotes.mapKeys(uri -> paxosExecutors().get(uri))
                .entries()
                .map(entry -> WithDedicatedExecutor.<T>of(entry.getValue(), entry.getKey()))
                .collect(Collectors.toList());
    }

    private <T> KeyedStream<String, T> createInstrumentedRemoteProxies(Class<T> clazz, boolean shouldRetry) {
        return KeyedStream.of(context().remoteUris())
                .map(uri ->
                        context().dialogueServiceProvider().createSingleNodeInstrumentedProxy(uri, clazz, shouldRetry))
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

    @Path("/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE + "/" + PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE)
    public interface TimelockSingleLeaderPaxosLearnerRpcClient extends PaxosLearner {}

    @Path("/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE + "/" + PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE)
    public interface TimelockSingleLeaderPaxosAcceptorRpcClient extends PaxosAcceptor {}
}
