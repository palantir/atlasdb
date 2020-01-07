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

import java.util.List;
import java.util.stream.Collectors;

import javax.ws.rs.Path;

import org.immutables.value.Value;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.RemotingClientConfigs;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.leader.PingableLeader;
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
        return createInstrumentedRemoteProxies(TimelockPaxosAcceptorRpcClient.class);
    }

    @Value.Derived
    public List<TimelockPaxosLearnerRpcClient> nonBatchTimestampLearner() {
        return createInstrumentedRemoteProxies(TimelockPaxosLearnerRpcClient.class);
    }

    @Value.Derived
    public List<TimelockSingleLeaderPaxosLearnerRpcClient> singleLeaderLearner() {
        return createInstrumentedRemoteProxies(TimelockSingleLeaderPaxosLearnerRpcClient.class);
    }

    @Value.Derived
    public List<TimelockSingleLeaderPaxosAcceptorRpcClient> singleLeaderAcceptor() {
        return createInstrumentedRemoteProxies(TimelockSingleLeaderPaxosAcceptorRpcClient.class);
    }

    @Value.Derived
    public List<BatchPaxosAcceptorRpcClient> batchAcceptor() {
        return createInstrumentedRemoteProxies(BatchPaxosAcceptorRpcClient.class);
    }

    @Value.Derived
    public List<BatchPaxosLearnerRpcClient> batchLearner() {
        return createInstrumentedRemoteProxies(BatchPaxosLearnerRpcClient.class);
    }

    @Value.Derived
    public List<PingableLeader> nonBatchPingableLeaders() {
        return createInstrumentedRemoteProxies(PingableLeader.class, false);
    }

    private <T> List<T> createInstrumentedRemoteProxies(Class<T> clazz) {
        return createInstrumentedRemoteProxies(clazz, true);
    }

    private <T> List<T> createInstrumentedRemoteProxies(Class<T> clazz, boolean shouldRetry) {
        return context().remoteUris().stream()
                .map(uri -> AtlasDbHttpClients.createProxy(
                        MetricsManagers.of(new MetricRegistry(), metrics()),
                        context().trustContext(),
                        uri,
                        clazz,
                        AuxiliaryRemotingParameters.builder()
                                .userAgent(context().userAgent())
                                .shouldLimitPayload(false)
                                .shouldRetry(shouldRetry)
                                .remotingClientConfig(() -> RemotingClientConfigs.ALWAYS_USE_CONJURE)
                                .build()))
                .map(proxy -> AtlasDbMetrics.instrumentWithTaggedMetrics(
                        metrics(),
                        clazz,
                        proxy,
                        MetricRegistry.name(clazz),
                        _unused -> ImmutableMap.of()))
                .collect(Collectors.toList());
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
