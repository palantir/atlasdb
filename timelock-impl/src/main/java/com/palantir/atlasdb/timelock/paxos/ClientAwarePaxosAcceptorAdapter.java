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

import com.codahale.metrics.MetricRegistry;
import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosPromise;
import com.palantir.paxos.PaxosProposal;
import com.palantir.paxos.PaxosProposalId;
import com.palantir.tritium.Tritium;

public class ClientAwarePaxosAcceptorAdapter implements PaxosAcceptor {

    private final String client;
    private final ClientAwarePaxosAcceptor clientAwarePaxosAcceptor;

    public ClientAwarePaxosAcceptorAdapter(
            String client,
            ClientAwarePaxosAcceptor clientAwarePaxosAcceptor,
            MetricRegistry metricRegistry) {
        this.client = client;
        this.clientAwarePaxosAcceptor = Tritium.instrument(ClientAwarePaxosAcceptor.class, clientAwarePaxosAcceptor, metricRegistry);
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
}
