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

import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.Maps;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.logsafe.Preconditions;
import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosPromise;
import com.palantir.paxos.PaxosProposal;
import com.palantir.paxos.PaxosProposalId;

public class BatchingPaxosAcceptorFactory {

    private final DisruptorAutobatcher<Map.Entry<Client, WithSeq<PaxosProposalId>>, PaxosPromise> prepareAutobatcher;
    private final DisruptorAutobatcher<Map.Entry<Client, PaxosProposal>, BooleanPaxosResponse> acceptAutobatcher;
    private final DisruptorAutobatcher<Client, Long> latestSequenceAutobatcher;

    private BatchingPaxosAcceptorFactory(
            DisruptorAutobatcher<Map.Entry<Client, WithSeq<PaxosProposalId>>, PaxosPromise> prepareAutobatcher,
            DisruptorAutobatcher<Map.Entry<Client, PaxosProposal>, BooleanPaxosResponse> acceptAutobatcher,
            DisruptorAutobatcher<Client, Long> latestSequenceAutobatcher) {
        this.prepareAutobatcher = prepareAutobatcher;
        this.acceptAutobatcher = acceptAutobatcher;
        this.latestSequenceAutobatcher = latestSequenceAutobatcher;
    }

    public BatchingPaxosAcceptorFactory create(BatchPaxosAcceptor batchPaxosAcceptor) {
        DisruptorAutobatcher<Map.Entry<Client, WithSeq<PaxosProposalId>>, PaxosPromise> prepareAutobatcher =
                Autobatchers.coalescing(new PrepareCoalescingFunction(batchPaxosAcceptor))
                        .safeLoggablePurpose("batch-paxos-acceptor-prepare")
                        .build();

        DisruptorAutobatcher<Map.Entry<Client, PaxosProposal>, BooleanPaxosResponse> acceptAutobatcher =
                Autobatchers.coalescing(new AcceptCoalescingFunction(batchPaxosAcceptor))
                .safeLoggablePurpose("batch-paxos-acceptor-accept")
                .build();

        DisruptorAutobatcher<Client, Long> latestSequenceAutobatcher =
                Autobatchers.coalescing(new BatchingPaxosLatestSequenceCache(batchPaxosAcceptor))
                .safeLoggablePurpose("batch-paxos-acceptor-latest-sequence-cache")
                .build();

        return new BatchingPaxosAcceptorFactory(prepareAutobatcher, acceptAutobatcher, latestSequenceAutobatcher);
    }

    public PaxosAcceptor paxosAcceptorForClient(Client client) {
        return new BatchingPaxosAcceptor(client);
    }

    private final class BatchingPaxosAcceptor implements PaxosAcceptor {

        private final Client client;

        private BatchingPaxosAcceptor(Client client) {
            this.client = client;
        }

        @Override
        public PaxosPromise prepare(long seq, PaxosProposalId pid) {
            try {
                return prepareAutobatcher.apply(Maps.immutableEntry(client, WithSeq.of(seq, pid))).get();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                throw new RuntimeException(cause);
            } catch (InterruptedException e) {
                // TODO(fdesouza): handle
                throw new RuntimeException(e);
            }
        }

        @Override
        public BooleanPaxosResponse accept(long seq, PaxosProposal proposal) {
            Preconditions.checkArgument(seq == proposal.getValue().getRound(), "seq does not match round in paxos value inside proposal");
            try {
                return acceptAutobatcher.apply(Maps.immutableEntry(client, proposal)).get();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                throw new RuntimeException(cause);
            } catch (InterruptedException e) {
                // TODO(fdesouza): handle
                throw new RuntimeException(e);
            }
        }

        @Override
        public long getLatestSequencePreparedOrAccepted() {
            try {
                return latestSequenceAutobatcher.apply(client).get();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                throw new RuntimeException(cause);
            } catch (InterruptedException e) {
                // TODO(fdesouza): handle
                throw new RuntimeException(e);
            }
        }
    }
}
