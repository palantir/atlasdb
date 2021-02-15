/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock.paxos;

import com.google.common.collect.Streams;
import com.palantir.atlasdb.timelock.paxos.BatchPaxosAcceptorRpcClient;
import com.palantir.atlasdb.timelock.paxos.PaxosRemoteClients;
import com.palantir.atlasdb.timelock.paxos.PaxosUseCase;
import com.palantir.atlasdb.timelock.paxos.WithDedicatedExecutor;
import com.palantir.common.proxy.PredicateSwitchedProxy;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosAcceptor;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class TimelockPaxosAcceptorAdapters {
    private TimelockPaxosAcceptorAdapters() {
        // pl0x n0
    }

    public static List<WithDedicatedExecutor<PaxosAcceptor>> create(
            PaxosUseCase paxosUseCase,
            PaxosRemoteClients remoteClients,
            Supplier<Boolean> useBatchedSingleLeader,
            Client client) {
        switch (paxosUseCase) {
            case LEADER_FOR_ALL_CLIENTS:
                return Streams.zip(
                                remoteClients.batchAcceptorsWithExecutors().stream(),
                                remoteClients.singleLeaderAcceptorsWithExecutors().stream(),
                                (batch, legacy) -> WithDedicatedExecutor.of(
                                        createSwitchingClient(
                                                batch.service(), legacy.service(), useBatchedSingleLeader),
                                        batch.executor()))
                        .collect(Collectors.toList());
            case LEADER_FOR_EACH_CLIENT:
                throw new SafeIllegalArgumentException("This should not be possible and is semantically meaningless");
            case TIMESTAMP:
                return remoteClients.nonBatchTimestampAcceptor().stream()
                        .map(acceptorAndExecutor -> WithDedicatedExecutor.<PaxosAcceptor>of(
                                new TimelockPaxosAcceptorAdapter(
                                        paxosUseCase, client.value(), acceptorAndExecutor.service()),
                                acceptorAndExecutor.executor()))
                        .collect(Collectors.toList());
            default:
                throw new IllegalStateException("Unexpected value: " + paxosUseCase);
        }
    }

    private static PaxosAcceptor createSwitchingClient(
            BatchPaxosAcceptorRpcClient batched,
            PaxosRemoteClients.TimelockSingleLeaderPaxosAcceptorRpcClient legacy,
            Supplier<Boolean> useBatched) {
        return PredicateSwitchedProxy.newProxyInstance(
                BatchTimelockPaxosAcceptorAdapter.singleLeader(batched), legacy, useBatched, PaxosAcceptor.class);
    }
}
