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

package com.palantir.timelock.paxos;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.palantir.atlasdb.timelock.paxos.Client;
import com.palantir.atlasdb.timelock.paxos.PaxosRemoteClients;
import com.palantir.atlasdb.timelock.paxos.PaxosUseCase;
import com.palantir.atlasdb.timelock.paxos.WithDedicatedExecutor;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosPromise;
import com.palantir.paxos.PaxosProposal;
import com.palantir.paxos.PaxosProposalId;

public final class TimelockPaxosAcceptorAdapter implements PaxosAcceptor {
    private final PaxosUseCase paxosUseCase;
    private final String client;
    private final TimelockPaxosAcceptorRpcClient timelockPaxosAcceptorRpcClient;

    TimelockPaxosAcceptorAdapter(
            PaxosUseCase paxosUseCase,
            String client,
            TimelockPaxosAcceptorRpcClient timelockPaxosAcceptorRpcClient) {
        this.paxosUseCase = paxosUseCase;
        this.client = client;
        this.timelockPaxosAcceptorRpcClient = timelockPaxosAcceptorRpcClient;
    }

    @Override
    public PaxosPromise prepare(long seq, PaxosProposalId pid) {
        return timelockPaxosAcceptorRpcClient.prepare(paxosUseCase, client, seq, pid);
    }

    @Override
    public BooleanPaxosResponse accept(long seq, PaxosProposal proposal) {
        return timelockPaxosAcceptorRpcClient.accept(paxosUseCase, client, seq, proposal);
    }

    @Override
    public long getLatestSequencePreparedOrAccepted() {
        return timelockPaxosAcceptorRpcClient.getLatestSequencePreparedOrAccepted(paxosUseCase, client);
    }

    /**
     * Given a list of {@link TimelockPaxosAcceptorRpcClient}s, returns a function allowing for injection of the client
     * name.
     */
    public static Function<Client, List<WithDedicatedExecutor<PaxosAcceptor>>> wrapWithDedicatedExecutors(
            PaxosUseCase paxosUseCase,
            PaxosRemoteClients remoteClients) {
        switch (paxosUseCase) {
            case LEADER_FOR_ALL_CLIENTS:
                return _client -> remoteClients
                        .singleLeaderAcceptorsWithExecutors()
                        .stream()
                        .map(remote -> remote.<PaxosAcceptor>transformService(x -> x))
                        .collect(Collectors.toList());
            case LEADER_FOR_EACH_CLIENT:
                throw new SafeIllegalArgumentException("This should not be possible and is semantically meaningless");
            case TIMESTAMP:
                throw new SafeIllegalArgumentException("Dedicated executors aren't currently supported for timestamp"
                        + " paxos.");
            default:
                throw new SafeIllegalStateException("Unexpected use case", SafeArg.of("paxosUseCase", paxosUseCase));
        }
    }

    public static Function<Client, List<PaxosAcceptor>> wrapWithoutDedicatedExecutors(
            PaxosUseCase paxosUseCase,
            PaxosRemoteClients remoteClients) {
        switch (paxosUseCase) {
            case LEADER_FOR_ALL_CLIENTS:
                throw new SafeIllegalArgumentException("Leadership acceptors must use dedicated executors to avoid"
                        + " thread explosion.");
            case LEADER_FOR_EACH_CLIENT:
                throw new SafeIllegalArgumentException("This should not be possible and is semantically meaningless");
            case TIMESTAMP:
                return client -> remoteClients.nonBatchTimestampAcceptor().stream()
                        .map(acceptor -> new TimelockPaxosAcceptorAdapter(
                                paxosUseCase,
                                client.value(),
                                acceptor))
                        .collect(Collectors.toList());
            default:
                throw new SafeIllegalStateException("Unexpected use case", SafeArg.of("paxosUseCase", paxosUseCase));
        }
    }
}
