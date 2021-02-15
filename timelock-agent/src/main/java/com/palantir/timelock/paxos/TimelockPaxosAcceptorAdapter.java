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

import com.palantir.atlasdb.timelock.paxos.PaxosUseCase;
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
            PaxosUseCase paxosUseCase, String client, TimelockPaxosAcceptorRpcClient timelockPaxosAcceptorRpcClient) {
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
}
