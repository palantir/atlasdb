/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.paxos;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public class PaxosLatestRoundVerifierImpl implements PaxosLatestRoundVerifier {
    private static final Logger log = LoggerFactory.getLogger(PaxosLatestRoundVerifierImpl.class);
    private static final double SAMPLE_RATE = 0.01;

    private final ImmutableList<PaxosAcceptor> acceptors;
    private final int quorumSize;
    private final Map<PaxosAcceptor, ExecutorService> executors;

    public PaxosLatestRoundVerifierImpl(
            List<PaxosAcceptor> acceptors, int quorumSize,
            Map<PaxosAcceptor, ExecutorService> executors) {
        this.acceptors = ImmutableList.copyOf(acceptors);
        this.quorumSize = quorumSize;
        this.executors = executors;
    }

    @Override
    public PaxosQuorumStatus isLatestRound(long round) {
        return collectResponses(round).getQuorumResult();
    }

    private PaxosResponses<PaxosResponse> collectResponses(long round) {
        return PaxosQuorumChecker.collectQuorumResponses(
                acceptors,
                acceptor -> new PaxosResponseImpl(acceptorAgreesIsLatestRound(acceptor, round)),
                quorumSize,
                executors,
                PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT);
    }

    private static boolean acceptorAgreesIsLatestRound(PaxosAcceptor acceptor, long round) {
        try {
            return round >= acceptor.getLatestSequencePreparedOrAccepted();
        } catch (Exception e) {
            if (log.isDebugEnabled() && shouldLog()) {
                log.debug("failed to get latest sequence", e);
            }
            throw e;
        }
    }

    private static boolean shouldLog() {
        return Math.random() < SAMPLE_RATE;
    }
}
