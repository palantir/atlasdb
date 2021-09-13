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

import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentSkipListMap;

public final class PaxosAcceptorImpl implements PaxosAcceptor {
    private static final SafeLogger log = SafeLoggerFactory.get(PaxosAcceptorImpl.class);

    public static PaxosAcceptor newAcceptor(String logDir) {
        PaxosStateLog<PaxosAcceptorState> stateLog = new PaxosStateLogImpl<>(logDir);
        return new PaxosAcceptorImpl(new ConcurrentSkipListMap<>(), stateLog, stateLog.getGreatestLogEntry());
    }

    public static PaxosAcceptor newSplittingAcceptor(
            PaxosStorageParameters params,
            SplittingPaxosStateLog.LegacyOperationMarkers legacyOperationMarkers,
            Optional<Long> migrateFrom) {
        PaxosStateLog<PaxosAcceptorState> stateLog = SplittingPaxosStateLog.createWithMigration(
                params,
                PaxosAcceptorState.BYTES_HYDRATOR,
                legacyOperationMarkers,
                migrateFrom.map(OptionalLong::of).orElseGet(OptionalLong::empty));
        return new PaxosAcceptorImpl(new ConcurrentSkipListMap<>(), stateLog, stateLog.getGreatestLogEntry());
    }

    private final ConcurrentSkipListMap<Long, PaxosAcceptorState> state;
    private final PaxosStateLog<PaxosAcceptorState> acceptorStateLog;
    private final long greatestInLogAtStartup;

    private PaxosAcceptorImpl(
            ConcurrentSkipListMap<Long, PaxosAcceptorState> state,
            PaxosStateLog<PaxosAcceptorState> acceptorStateLog,
            long greatestInLogAtStartup) {
        this.state = state;
        this.acceptorStateLog = acceptorStateLog;
        this.greatestInLogAtStartup = greatestInLogAtStartup;
    }

    @Override
    public PaxosPromise prepare(long seq, PaxosProposalId pid) {
        try {
            checkLogIfNeeded(seq);
        } catch (Exception e) {
            log.error("log read failed for request: {}", SafeArg.of("seq", seq), e);
            return PaxosPromise.reject(pid);
        }

        for (; ; ) {
            PaxosAcceptorState oldState = state.get(seq);

            if (oldState != null && pid.compareTo(oldState.lastPromisedId) < 0) {
                return PaxosPromise.reject(oldState.lastPromisedId);
            }

            // allow for the same propose to be repeated and return the same result.
            if (oldState != null && pid.compareTo(oldState.lastPromisedId) == 0) {
                return PaxosPromise.accept(
                        oldState.lastPromisedId, oldState.lastAcceptedId, oldState.lastAcceptedValue);
            }

            PaxosAcceptorState newState =
                    oldState != null ? oldState.withPromise(pid) : PaxosAcceptorState.newState(pid);
            if ((oldState == null && state.putIfAbsent(seq, newState) == null)
                    || (oldState != null && state.replace(seq, oldState, newState))) {
                acceptorStateLog.writeRound(seq, newState);
                return PaxosPromise.accept(
                        newState.lastPromisedId, newState.lastAcceptedId, newState.lastAcceptedValue);
            }
        }
    }

    @Override
    public BooleanPaxosResponse accept(long seq, PaxosProposal proposal) {
        try {
            checkLogIfNeeded(seq);
        } catch (Exception e) {
            log.error("Log read failed for request at sequence {}", SafeArg.of("sequence", seq), e);
            return new BooleanPaxosResponse(false); // nack
        }

        for (; ; ) {
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
                acceptorStateLog.writeRound(seq, newState);
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

        if (seq < acceptorStateLog.getLeastLogEntry()) {
            throw new TruncatedStateLogException(
                    "round " + seq + " before truncation cutoff of " + acceptorStateLog.getLeastLogEntry());
        }

        if (seq <= acceptorStateLog.getGreatestLogEntry()) {
            byte[] bytes = acceptorStateLog.readRound(seq);
            if (bytes != null) {
                state.put(seq, PaxosAcceptorState.BYTES_HYDRATOR.hydrateFromBytes(bytes));
            }
        }
    }
}
