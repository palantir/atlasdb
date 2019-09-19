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

import com.palantir.atlasdb.timelock.paxos.Client;
import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosPromise;
import com.palantir.paxos.PaxosProposal;
import com.palantir.paxos.PaxosProposalId;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;


public final class ClientAwarePaxosAcceptorAdapter implements PaxosAcceptor {
    private final String client;
    private final ClientAwarePaxosAcceptor clientAwarePaxosAcceptor;

    private ClientAwarePaxosAcceptorAdapter(Client client, ClientAwarePaxosAcceptor clientAwarePaxosAcceptor) {
        this.client = client.value();
        this.clientAwarePaxosAcceptor = clientAwarePaxosAcceptor;
    }

    @Override
    public PaxosPromise prepare(long seq, PaxosProposalId pid) {
        return clientAwarePaxosAcceptor.prepare(client, seq, pid);
    }

    @Override
    public BooleanPaxosResponse accept(long seq, PaxosProposal proposal) {
        return clientAwarePaxosAcceptor.accept(client, seq, proposal);
    }

    @Override
    public long getLatestSequencePreparedOrAccepted() {
        return clientAwarePaxosAcceptor.getLatestSequencePreparedOrAccepted(client);
    }

    /**
     * Given a list of {@link ClientAwarePaxosAcceptor}s, returns a function allowing for injection of the client name.
     */
    public static Function<Client, List<PaxosAcceptor>> wrap(List<ClientAwarePaxosAcceptor> acceptors) {
        return client -> acceptors.stream()
                .map(acceptor -> new ClientAwarePaxosAcceptorAdapter(client, acceptor))
                .collect(Collectors.toList());
    }
}
