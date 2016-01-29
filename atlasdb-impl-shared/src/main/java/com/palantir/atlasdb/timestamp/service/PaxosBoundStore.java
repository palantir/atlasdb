/**
 * Copyright 2016 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.timestamp.service;

import java.util.List;
import java.util.concurrent.Executor;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosQuorumChecker;
import com.palantir.paxos.PaxosQuorumChecker.PaxosQuorumResponse;
import com.palantir.paxos.PaxosResponse;
import com.palantir.paxos.PaxosRoundFailureException;
import com.palantir.paxos.PaxosValue;
import com.palantir.timestamp.MultipleRunningTimestampServiceException;
import com.palantir.timestamp.TimestampBoundStore;

public class PaxosBoundStore implements TimestampBoundStore {
    private static final long INITIAL_VALUE = 10000L;

    final PaxosProposer proposer;
    final PaxosLearner localLearner;
    long currentOwnedSeq;
    long currentLimit;

    public static TimestampBoundStore create(PaxosProposer proposer,
                                             List<PaxosAcceptor> acceptors,
                                             PaxosLearner localLearner,
                                             Executor executor) {
        PaxosQuorumResponse<PaxosConstant> responses = PaxosQuorumChecker.<PaxosAcceptor, PaxosConstant> collectQuorumResponses(
                acceptors,
                new Function<PaxosAcceptor, PaxosConstant>() {
                    @Override
                    @Nullable
                    public PaxosConstant apply(@Nullable PaxosAcceptor acceptor) {
                        long latestSequence = acceptor.getLatestSequencePreparedOrAccepted();
                        return new PaxosConstant(latestSequence);
                    }
                },
                proposer.getQuorumSize(),
                executor,
                PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT_IN_SECONDS);
        if (!responses.hasQuorum()) {
            throw new ServiceNotAvailableException("failed to get a quorum");
        }
        try {
            long maxDiscussedSeq = Ordering.natural().max(Iterables.transform(responses.getResponses(), PaxosConstant.getValueFun()));
            long seqToCheck = maxDiscussedSeq;
            while (seqToCheck >= 0) {
                long seqValue = PtBytes.toLong(proposer.propose(seqToCheck, PtBytes.toBytes(-1)));
                if (seqValue >= 0) {
                    return createStoreWithStartingPoint(proposer, localLearner, maxDiscussedSeq + 1, seqValue);
                }
                seqToCheck--;
            }
            return createStoreWithStartingPoint(proposer, localLearner, maxDiscussedSeq + 1, INITIAL_VALUE);
        } catch (PaxosRoundFailureException e) {
            throw new ServiceNotAvailableException("failed to get a quorum", e);
        }
    }

    static TimestampBoundStore createStoreWithStartingPoint(PaxosProposer proposer, PaxosLearner learner, long seqToTake, long valueToStore) throws PaxosRoundFailureException {
        long agreedValue = PtBytes.toLong(proposer.propose(seqToTake, PtBytes.toBytes(valueToStore)));
        if (agreedValue != valueToStore) {
            throw new NotCurrentLeaderException("not leading");
        }
        PaxosValue learnedValue = learner.getLearnedValue(seqToTake);
        if (!learnedValue.getLeaderUUID().equals(proposer.getUUID())) {
            throw new NotCurrentLeaderException("not leading");
        }
        return new PaxosBoundStore(proposer, learner, seqToTake, valueToStore);
    }



    private PaxosBoundStore(PaxosProposer proposer,
                           PaxosLearner localLearner,
                           long currentOwnedSeq,
                           long currentLimit) {
        this.proposer = proposer;
        this.localLearner = localLearner;
        this.currentOwnedSeq = currentOwnedSeq;
        this.currentLimit = currentLimit;
    }

    static class PaxosConstant implements PaxosResponse {
        private static final long serialVersionUID = 1L;

        final long value;

        public static Function<PaxosConstant, Long> getValueFun() {
            return new Function<PaxosBoundStore.PaxosConstant, Long>() {
                @Override
                public Long apply(PaxosConstant input) {
                    return input.value;
                }
            };
        }

        public PaxosConstant(long value) {
            this.value = value;
        }

        @Override
        public boolean isSuccessful() {
            return true;
        }

    }

    @Override
    public long getUpperLimit() {
        return currentLimit;
    }

    @Override
    public synchronized void storeUpperLimit(long limit) throws MultipleRunningTimestampServiceException {
        try {
            long agreed = PtBytes.toLong(proposer.propose(currentOwnedSeq + 1, PtBytes.toBytes(limit)));
            if (agreed != limit) {
                throw new MultipleRunningTimestampServiceException("Another server has changed the value out from under us.");
            }
            PaxosValue learned = localLearner.getLearnedValue(currentOwnedSeq + 1);
            if (!learned.getLeaderUUID().equals(proposer.getUUID())) {
                throw new MultipleRunningTimestampServiceException("Another server has changed the value out from under us.");
            }
            currentOwnedSeq++;
            currentLimit = limit;
        } catch (PaxosRoundFailureException e) {
            throw new ServiceNotAvailableException("failed when trying to store bounds", e);
        }
    }

}
