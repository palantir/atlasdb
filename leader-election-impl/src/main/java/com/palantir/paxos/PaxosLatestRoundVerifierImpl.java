/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.paxos;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;

public class PaxosLatestRoundVerifierImpl implements PaxosLatestRoundVerifier {

    private final ImmutableList<PaxosAcceptor> acceptors;
    private final int quorumSize;
    private final ExecutorService executor;
    private final Supplier<Boolean> onlyLogOnQuorumFailure;

    public PaxosLatestRoundVerifierImpl(
            List<PaxosAcceptor> acceptors, int quorumSize,
            ExecutorService executor, Supplier<Boolean> onlyLogOnQuorumFailure) {
        this.acceptors = ImmutableList.copyOf(acceptors);
        this.quorumSize = quorumSize;
        this.executor = executor;
        this.onlyLogOnQuorumFailure = onlyLogOnQuorumFailure;
    }

    @Override
    public PaxosQuorumStatus isLatestRound(long round) {
        List<PaxosResponse> responses = collectResponses(round);

        return determineQuorumStatus(responses);
    }

    private List<PaxosResponse> collectResponses(long round) {
        return PaxosQuorumChecker.collectQuorumResponses(
                acceptors,
                acceptor -> new PaxosResponseImpl(acceptorAgreesIsLatestRound(acceptor, round)),
                quorumSize,
                executor,
                PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT_IN_SECONDS,
                onlyLogOnQuorumFailure.get());
    }

    private boolean acceptorAgreesIsLatestRound(PaxosAcceptor acceptor, long round) {
        return round >= acceptor.getLatestSequencePreparedOrAccepted();
    }

    private PaxosQuorumStatus determineQuorumStatus(List<PaxosResponse> responses) {
        return PaxosQuorumChecker.getQuorumResult(responses, quorumSize);
    }

}
