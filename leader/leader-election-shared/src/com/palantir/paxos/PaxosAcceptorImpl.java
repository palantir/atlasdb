// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.paxos;

import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaxosAcceptorImpl implements PaxosAcceptor {
    private static final Logger logger = LoggerFactory.getLogger(PaxosAcceptorImpl.class);

    /**
     * @param logDir string path for directory to place durable logs
     * @param type the type of the objects accepted by the acceptor
     * @return a new acceptor
     */
    public static PaxosAcceptor newAcceptor(String logDir) {
        PaxosStateLog<PaxosAcceptorState> log = new PaxosStateLogImpl<PaxosAcceptorState>(logDir);
        return new PaxosAcceptorImpl(
                new ConcurrentSkipListMap<Long, PaxosAcceptorState>(),
                log,
                log.getGreatestLogEntry());
    }

    final ConcurrentSkipListMap<Long, PaxosAcceptorState> state;
    final PaxosStateLog<PaxosAcceptorState> log;
    final long greatestInLogAtStartup;

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
            logger.error("log read failed for request: " + seq, e);
            return new PaxosPromise(pid); // nack
        }

        for (;;) {
            PaxosAcceptorState oldState = state.get(seq);

            // nack
            if (oldState != null && pid.compareTo(oldState.lastPromisedId) <= 0) {
                return new PaxosPromise(oldState.lastPromisedId);
            }

            // ack
            PaxosAcceptorState newState = oldState != null
                    ? oldState.withPromise(pid)
                    : PaxosAcceptorState.newState(pid);
            if ((oldState == null && state.putIfAbsent(seq, newState) == null)
                    || (oldState != null && state.replace(seq, oldState, newState))) {
                log.writeRound(seq, newState);
                return new PaxosPromise(
                        newState.lastPromisedId,
                        newState.lastAcceptedId,
                        newState.lastAcceptedValue);
            }
        }
    }

    @Override
    public PaxosResponse accept(long seq, PaxosProposal proposal) {
        try {
            checkLogIfNeeded(seq);
        } catch (Exception e) {
            logger.error("log read failed for request: " + seq, e);
            return new PaxosResponseImpl(false); // nack
        }

        for (;;) {
            PaxosAcceptorState oldState = state.get(seq);

            // nack
            if (oldState != null && proposal.id.compareTo(oldState.lastPromisedId) < 0) {
                return new PaxosResponseImpl(false);
            }

            // ack
            PaxosAcceptorState newState = oldState.withState(proposal.id, proposal.id, proposal.val);
            if ((oldState == null && state.putIfAbsent(seq, newState) == null)
                    || (oldState != null && state.replace(seq, oldState, newState))) {
                log.writeRound(seq, newState);
                return new PaxosResponseImpl(true);
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
