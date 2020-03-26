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

import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

import javax.ws.rs.Path;

import org.immutables.value.Value;

import com.google.common.net.HostAndPort;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.RemotingClientConfigs;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.common.streams.KeyedStream;
import com.palantir.leader.PingableLeader;
import com.palantir.paxos.ImmutableLeaderPingerContext;
import com.palantir.paxos.LeaderPingerContext;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.timelock.paxos.TimelockPaxosAcceptorRpcClient;
import com.palantir.timelock.paxos.TimelockPaxosLearnerRpcClient;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

@Value.Immutable
public abstract class PaxosRemoteClients {

    @Value.Parameter
    public abstract PaxosResourcesFactory.TimelockPaxosInstallationContext context();

    @Value.Parameter
    public abstract TaggedMetricRegistry metrics();

    @Value.Derived
    public List<TimelockPaxosAcceptorRpcClient> nonBatchTimestampAcceptor() {
        return createInstrumentedRemoteProxyList(TimelockPaxosAcceptorRpcClient.class, true);
    }

    @Value.Derived
    public List<TimelockPaxosLearnerRpcClient> nonBatchTimestampLearner() {
        return createInstrumentedRemoteProxyList(TimelockPaxosLearnerRpcClient.class, true);
    }

    @Value.Derived
    public List<TimelockSingleLeaderPaxosAcceptorRpcClient> singleLeaderAcceptor() {
        // Retries should be performed at a higher level, in AwaitingLeadershipProxy.
        return createInstrumentedRemoteProxyList(TimelockSingleLeaderPaxosAcceptorRpcClient.class, false);
    }

    @Value.Derived
    public List<TimelockSingleLeaderPaxosLearnerRpcClient> singleLeaderLearner() {
        return createInstrumentedRemoteProxyList(TimelockSingleLeaderPaxosLearnerRpcClient.class, true);
    }

    @Value.Derived
    public List<BatchPaxosAcceptorRpcClient> batchAcceptor() {
        return createInstrumentedRemoteProxyList(BatchPaxosAcceptorRpcClient.class, false);
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
        return createInstrumentedRemoteProxies(clazz, false).entries()
                .<LeaderPingerContext<T>>map(entry -> ImmutableLeaderPingerContext.of(entry.getValue(), entry.getKey()))
                .collect(Collectors.toList());
    }

    private <T> List<T> createInstrumentedRemoteProxyList(Class<T> clazz, boolean shouldRetry) {
        return createInstrumentedRemoteProxies(clazz, shouldRetry).values().collect(Collectors.toList());
    }

    private <T> KeyedStream<HostAndPort, T> createInstrumentedRemoteProxies(Class<T> clazz, boolean shouldRetry) {
        return KeyedStream.of(context().remoteUris())
                .map(uri -> AtlasDbHttpClients.createProxy(
                        context().trustContext(),
                        uri,
                        clazz,
                        AuxiliaryRemotingParameters.builder()
                                .userAgent(context().userAgent())
                                .shouldLimitPayload(false)
                                .shouldRetry(shouldRetry)
                                .remotingClientConfig(() -> RemotingClientConfigs.DEFAULT)
                                .shouldSupportBlockingOperations(false)
                                .build()))
                .map(proxy -> AtlasDbMetrics.instrumentWithTaggedMetrics(metrics(), clazz, proxy))
                .mapKeys(PaxosRemoteClients::convertAddressToHostAndPort);
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
