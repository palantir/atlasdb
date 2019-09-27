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

import org.immutables.value.Value;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.util.AtlasDbMetrics;
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
    public List<TimelockPaxosAcceptorRpcClient> nonBatchAcceptor() {
        return createInstrumentedRemoteProxies(TimelockPaxosAcceptorRpcClient.class, "paxos-acceptor-rpc-client");
    }

    @Value.Derived
    public List<TimelockPaxosLearnerRpcClient> nonBatchLearner() {
        return createInstrumentedRemoteProxies(TimelockPaxosLearnerRpcClient.class, "paxos-learner-rpc-client");
    }

    @Value.Derived
    public List<BatchPaxosAcceptorRpcClient> batchAcceptor() {
        return createInstrumentedRemoteProxies(
                BatchPaxosAcceptorRpcClient.class,
                "batch-paxos-acceptor-rpc-client");
    }

    @Value.Derived
    public List<BatchPaxosLearnerRpcClient> batchLearner() {
        return createInstrumentedRemoteProxies(
                BatchPaxosLearnerRpcClient.class,
                "batch-paxos-learner-rpc-client");
    }

    private <T> List<T> createInstrumentedRemoteProxies(Class<T> clazz, String name) {
        return context().remoteUris().stream()
                // TODO(fdesouza): wire up the configurable cutover to CJR
                .map(uri -> AtlasDbHttpClients.DEFAULT_TARGET_FACTORY.createProxy(
                        context().trustContext(),
                        uri,
                        clazz,
                        name,
                        false))
                .map(proxy -> AtlasDbMetrics.instrumentWithTaggedMetrics(
                        metrics(),
                        clazz,
                        proxy,
                        name,
                        _unused -> ImmutableMap.of()))
                .collect(Collectors.toList());
    }
}
