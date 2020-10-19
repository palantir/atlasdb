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

import static com.palantir.atlasdb.timelock.paxos.PaxosQuorumCheckingCoalescingFunction.wrap;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.common.concurrent.CheckedRejectionExecutorService;
import com.palantir.logsafe.Preconditions;
import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosAcceptorNetworkClient;
import com.palantir.paxos.PaxosLong;
import com.palantir.paxos.PaxosPromise;
import com.palantir.paxos.PaxosProposal;
import com.palantir.paxos.PaxosProposalId;
import com.palantir.paxos.PaxosResponses;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public final class AutobatchingPaxosAcceptorNetworkClientFactory implements Closeable {

    private final DisruptorAutobatcher<Map.Entry<Client, WithSeq<PaxosProposalId>>, PaxosResponses<PaxosPromise>>
            prepare;
    private final DisruptorAutobatcher<Map.Entry<Client, PaxosProposal>, PaxosResponses<BooleanPaxosResponse>> accept;
    private final DisruptorAutobatcher<Client, PaxosResponses<PaxosLong>> latestSequence;

    private AutobatchingPaxosAcceptorNetworkClientFactory(
            DisruptorAutobatcher<Map.Entry<Client, WithSeq<PaxosProposalId>>, PaxosResponses<PaxosPromise>> prepare,
            DisruptorAutobatcher<Map.Entry<Client, PaxosProposal>, PaxosResponses<BooleanPaxosResponse>> accept,
            DisruptorAutobatcher<Client, PaxosResponses<PaxosLong>> latestSequence) {
        this.prepare = prepare;
        this.accept = accept;
        this.latestSequence = latestSequence;
    }

    public static AutobatchingPaxosAcceptorNetworkClientFactory create(
            List<BatchPaxosAcceptor> acceptors,
            Map<BatchPaxosAcceptor, CheckedRejectionExecutorService> executors,
            int quorumSize) {

        DisruptorAutobatcher<Map.Entry<Client, WithSeq<PaxosProposalId>>, PaxosResponses<PaxosPromise>> prepare =
                Autobatchers.coalescing(wrap(acceptors, executors, quorumSize, PrepareCoalescingFunction::new))
                        .safeLoggablePurpose("batch-paxos-acceptor.prepare")
                        .build();

        DisruptorAutobatcher<Map.Entry<Client, PaxosProposal>, PaxosResponses<BooleanPaxosResponse>> accept =
                Autobatchers.coalescing(wrap(acceptors, executors, quorumSize, AcceptCoalescingFunction::new))
                        .safeLoggablePurpose("batch-paxos-acceptor.accept")
                        .build();

        DisruptorAutobatcher<Client, PaxosResponses<PaxosLong>> latestSequenceAutobatcher = Autobatchers.coalescing(
                        wrap(acceptors, executors, quorumSize, BatchingPaxosLatestSequenceCache::new))
                .safeLoggablePurpose("batch-paxos-acceptor.latest-sequence-cache")
                .build();

        return new AutobatchingPaxosAcceptorNetworkClientFactory(prepare, accept, latestSequenceAutobatcher);
    }

    public PaxosAcceptorNetworkClient paxosAcceptorForClient(Client client) {
        return new AutobatchingPaxosAcceptorNetworkClient(client);
    }

    @Override
    public void close() {
        prepare.close();
        accept.close();
        latestSequence.close();
    }

    private final class AutobatchingPaxosAcceptorNetworkClient implements PaxosAcceptorNetworkClient {

        private final Client client;

        private AutobatchingPaxosAcceptorNetworkClient(Client client) {
            this.client = client;
        }

        @Override
        public PaxosResponses<PaxosPromise> prepare(long seq, PaxosProposalId proposalId) {
            try {
                return prepare.apply(Maps.immutableEntry(client, WithSeq.of(proposalId, seq)))
                        .get();
            } catch (ExecutionException | InterruptedException e) {
                throw AutobatcherExecutionExceptions.handleAutobatcherExceptions(e);
            }
        }

        @Override
        public PaxosResponses<BooleanPaxosResponse> accept(long seq, PaxosProposal proposal) {
            Preconditions.checkArgument(
                    seq == proposal.getValue().getRound(), "seq does not match round in paxos value inside proposal");
            try {
                return accept.apply(Maps.immutableEntry(client, proposal)).get();
            } catch (ExecutionException | InterruptedException e) {
                throw AutobatcherExecutionExceptions.handleAutobatcherExceptions(e);
            }
        }

        @Override
        public PaxosResponses<PaxosLong> getLatestSequencePreparedOrAccepted() {
            try {
                return latestSequence.apply(client).get();
            } catch (ExecutionException | InterruptedException e) {
                throw AutobatcherExecutionExceptions.handleAutobatcherExceptions(e);
            }
        }

        @Override
        public ListenableFuture<PaxosResponses<PaxosLong>> getLatestSequencePreparedOrAcceptedAsync() {
            return latestSequence.apply(client);
        }
    }
}
