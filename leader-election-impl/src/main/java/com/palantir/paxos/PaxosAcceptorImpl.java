/**
 * Copyright 2015 Palantir Technologies
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

import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaxosAcceptorImpl implements PaxosAcceptor {
    private static final Logger logger = LoggerFactory.getLogger(PaxosAcceptorImpl.class);
    private static final Logger leaderLog = LoggerFactory.getLogger("leadership");

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
        leaderLog.debug("Received proposal request for seq #" + seq + " with ID " + pid.getNumber() + " from node " + pid.getProposerUUID());

        try {
            checkLogIfNeeded(seq);
        } catch (Exception e) {
            leaderLog.error("log read failed for request: " + seq + " during prepare phase; rejecting proposal request", e);
            logger.error("log read failed for request: " + seq, e);
            return new PaxosPromise(pid); // nack
        }

        for (;;) {
            PaxosAcceptorState oldState = state.get(seq);

            // nack
            if (oldState != null && pid.compareTo(oldState.lastPromisedId) < 0) {
                leaderLog.debug("Refused proposal request for seq #{} with ID {} as we've already made a promise with a greater pid ({})",
                        seq, pid.getNumber(), oldState.lastPromisedId);
                return new PaxosPromise(oldState.lastPromisedId);
            }

            // allow for the same propose to be repeated and return the same result.
            if (oldState != null && pid.compareTo(oldState.lastPromisedId) == 0) {
                leaderLog.debug("Accepted proposal request for seq #{} with ID {} as we've already made a promise with the same pid ({}) " +
                        "for leader UUID {}", seq, pid.getNumber(), oldState.lastPromisedId, oldState.lastAcceptedValue.getLeaderUUID());
                return new PaxosPromise(
                        oldState.lastPromisedId,
                        oldState.lastAcceptedId,
                        oldState.lastAcceptedValue);
            }

            // ack
            PaxosAcceptorState newState = oldState != null
                    ? oldState.withPromise(pid)
                    : PaxosAcceptorState.newState(pid);
            if ((oldState == null && state.putIfAbsent(seq, newState) == null)
                    || (oldState != null && state.replace(seq, oldState, newState))) {
                leaderLog.debug("Promised to accept proposal for seq #{} with ID {}", seq, pid.getNumber());
                log.writeRound(seq, newState);
                return new PaxosPromise(
                        newState.lastPromisedId,
                        newState.lastAcceptedId,
                        newState.lastAcceptedValue);
            }
        }
    }

    @Override
    public BooleanPaxosResponse accept(long seq, PaxosProposal proposal) {
        leaderLog.debug("Asked to accept proposal for seq #{} with ID {} from node {}",
                seq, proposal.getId().getNumber(), proposal.getId().getProposerUUID());

        try {
            checkLogIfNeeded(seq);
        } catch (Exception e) {
            leaderLog.error("log read failed for request: {} during accept phase; rejecting proposal", seq, e);
            logger.error("log read failed for request: " + seq, e);
            return new BooleanPaxosResponse(false); // nack
        }

        for (;;) {
            PaxosAcceptorState oldState = state.get(seq);

            // nack
            if (oldState != null && proposal.id.compareTo(oldState.lastPromisedId) < 0) {
                leaderLog.debug("Rejected proposal for seq #{} with ID {} as we already promised to accept ID {}",
                        seq, proposal.getId().getNumber(), oldState.lastPromisedId);
                return new BooleanPaxosResponse(false);
            }

            // ack
            PaxosAcceptorState newState = oldState != null
                    ? oldState.withState(proposal.id, proposal.id, proposal.val)
                    : PaxosAcceptorState.newState(proposal.id);
            if ((oldState == null && state.putIfAbsent(seq, newState) == null)
                    || (oldState != null && state.replace(seq, oldState, newState))) {
                leaderLog.info("Accepted proposal for seq #{} with ID {}; proposed leader UUID is {}",
                        seq, proposal.getId().getNumber(), proposal.getValue().getLeaderUUID());
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
