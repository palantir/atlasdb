/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock.history.utils;

import com.palantir.paxos.PaxosAcceptorState;
import com.palantir.paxos.PaxosProposalId;
import com.palantir.paxos.PaxosStateLog;
import com.palantir.paxos.PaxosValue;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class PaxosSerializationTestUtils {

    private PaxosSerializationTestUtils() {
        // no op
    }

    public static Set<PaxosValue> writeToLogs(
            PaxosStateLog<PaxosAcceptorState> acceptorLog,
            PaxosStateLog<PaxosValue> learnerLog,
            int startInclusive,
            int endInclusive) {
        return IntStream.rangeClosed(startInclusive, endInclusive)
                .boxed()
                .map(i -> {
                    PaxosValue paxosValue = createPaxosValueForRound(i);
                    writeAcceptorStateForLogAndRound(acceptorLog, i, Optional.of(paxosValue));
                    writePaxosValue(learnerLog, i, paxosValue);
                    return paxosValue;
                })
                .collect(Collectors.toSet());
    }

    public static PaxosValue createAndWriteValueForLogAndRound(PaxosStateLog<PaxosValue> log, long round) {
        PaxosValue paxosValue = createPaxosValueForRound(round);
        return writePaxosValue(log, round, paxosValue);
    }

    public static PaxosValue writePaxosValue(PaxosStateLog<PaxosValue> log, long round, PaxosValue paxosValue) {
        log.writeRound(round, paxosValue);
        return paxosValue;
    }

    public static PaxosValue createPaxosValueForRound(long round) {
        return new PaxosValue("leaderUuid", round, longToBytes(round));
    }

    public static PaxosValue createPaxosValueForRoundAndData(long round, long data) {
        return new PaxosValue("leaderUuid", round, longToBytes(data));
    }

    public static PaxosAcceptorState writeAcceptorStateForLogAndRound(
            PaxosStateLog<PaxosAcceptorState> log, long round, Optional<PaxosValue> paxosValue) {
        PaxosAcceptorState acceptorState = getAcceptorStateForRound(round, paxosValue);
        log.writeRound(round, acceptorState);
        return acceptorState;
    }

    private static PaxosAcceptorState getAcceptorStateForRound(long round, Optional<PaxosValue> paxosValue) {
        PaxosProposalId pid = new PaxosProposalId(round, UUID.randomUUID().toString());
        PaxosAcceptorState acceptorState = PaxosAcceptorState.newState(pid);
        return paxosValue.map(val -> acceptorState.withState(pid, pid, val)).orElse(acceptorState);
    }

    public static byte[] longToBytes(long value) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        return buffer.array();
    }
}
