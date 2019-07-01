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

import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.logsafe.SafeArg;

public final class PaxosAcceptorImpl implements PaxosAcceptor {
    private static final Logger logger = LoggerFactory.getLogger(PaxosAcceptorImpl.class);

    public static PaxosAcceptor newAcceptor(String logDir) {
        PaxosStateLog<PaxosAcceptorState> log = new PaxosStateLogImpl<>(logDir);
        return new PaxosAcceptorImpl(
                new ConcurrentSkipListMap<>(),
                log,
                log.getGreatestLogEntry());
    }

    private final ConcurrentSkipListMap<Long, PaxosAcceptorState> state;
    private final PaxosStateLog<PaxosAcceptorState> log;
    private final long greatestInLogAtStartup;

    private PaxosAcceptorImpl(ConcurrentSkipListMap<Long, PaxosAcceptorState> state,
                              PaxosStateLog<PaxosAcceptorState> log,
                              long greatestInLogAtStartup) {
        this.state = state;
        this.log = log;
        this.greatestInLogAtStartup = greatestInLogAtStartup;
    }

    @Override
    public PaxosPromise prepare(long seq, PaxosProposalId pid) {
        try {
            checkLogIfNeeded(seq);
        } catch (Exception e) {
            logger.error("log read failed for request: {}", seq, e);
            return PaxosPromise.reject(pid);
        }

        for (;;) {
            PaxosAcceptorState oldState = state.get(seq);

            if (oldState != null && pid.compareTo(oldState.lastPromisedId) < 0) {
                return PaxosPromise.reject(oldState.lastPromisedId);
            }

            // allow for the same propose to be repeated and return the same result.
            if (oldState != null && pid.compareTo(oldState.lastPromisedId) == 0) {
                return PaxosPromise.accept(
                        oldState.lastPromisedId,
                        oldState.lastAcceptedId,
                        oldState.lastAcceptedValue);
            }

            PaxosAcceptorState newState = oldState != null
                    ? oldState.withPromise(pid)
                    : PaxosAcceptorState.newState(pid);
            if ((oldState == null && state.putIfAbsent(seq, newState) == null)
                    || (oldState != null && state.replace(seq, oldState, newState))) {
                log.writeRound(seq, newState);
                return PaxosPromise.accept(
                        newState.lastPromisedId,
                        newState.lastAcceptedId,
                        newState.lastAcceptedValue);
            }
        }
    }

    @Override
    public BooleanPaxosResponse accept(long seq, PaxosProposal proposal) {
        try {
            checkLogIfNeeded(seq);
        } catch (Exception e) {
            logger.error("Log read failed for request at sequence {}", SafeArg.of("sequence", seq), e);
            return new BooleanPaxosResponse(false); // nack
        }

        for (;;) {
            PaxosAcceptorState oldState = state.get(seq);

            // nack
            if (oldState != null && proposal.id.compareTo(oldState.lastPromisedId) < 0) {
                return new BooleanPaxosResponse(false);
            }

            // ack
            PaxosAcceptorState newState = oldState != null
                    ? oldState.withState(proposal.id, proposal.id, proposal.val)
                    : PaxosAcceptorState.newState(proposal.id);
            if ((oldState == null && state.putIfAbsent(seq, newState) == null)
                    || (oldState != null && state.replace(seq, oldState, newState))) {
                log.writeRound(seq, newState);
                return new BooleanPaxosResponse(true);
            }
        }
    }

    @Override
    public long getLatestSequencePreparedOrAccepted() {
        if (state.isEmpty()) {
            return greatestInLogAtStartup;
        } else {
            return Math.max(greatestInLogAtStartup, state.lastKey());
        }
    }

    private void checkLogIfNeeded(long seq) throws TruncatedStateLogException, IOException {
        if (state.containsKey(seq)) {
            return;
        }

        if (seq < log.getLeastLogEntry()) {
            throw new TruncatedStateLogException("round " + seq + " before truncation cutoff of "
                    + log.getLeastLogEntry());
        }

        if (seq <= log.getGreatestLogEntry()) {
            byte[] bytes = log.readRound(seq);
            if (bytes != null) {
                state.put(seq, PaxosAcceptorState.BYTES_HYDRATOR.hydrateFromBytes(bytes));
            }
        }
    }

}
