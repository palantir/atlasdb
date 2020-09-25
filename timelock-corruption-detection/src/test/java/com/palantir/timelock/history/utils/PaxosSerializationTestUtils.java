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

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.palantir.paxos.PaxosAcceptorState;
import com.palantir.paxos.PaxosProposalId;
import com.palantir.paxos.PaxosStateLog;
import com.palantir.paxos.PaxosValue;

public final class PaxosSerializationTestUtils {

    private PaxosSerializationTestUtils() {
        // no op
    }

    public static Set<PaxosValue> writeToLogs(PaxosStateLog<PaxosAcceptorState> acceptorLog,
            PaxosStateLog<PaxosValue> learnerLog, int start, int end) {
        return IntStream.rangeClosed(start, end).boxed().map(i -> {
            writeAcceptorStateForLogAndRound(acceptorLog, i);
            return writeValueForLogAndRound(learnerLog, i);
        }).collect(Collectors.toSet());
    }

    public static PaxosValue writeValueForLogAndRound(PaxosStateLog<PaxosValue> log, long round) {
        PaxosValue paxosValue = new PaxosValue("leaderUuid", round, longToBytes(round));
        log.writeRound(round, paxosValue);
        return paxosValue;
    }

    public static PaxosAcceptorState writeAcceptorStateForLogAndRound(
            PaxosStateLog<PaxosAcceptorState> log, long round) {
        PaxosAcceptorState acceptorState = getAcceptorStateForRound(round);
        log.writeRound(round, acceptorState);
        return acceptorState;
    }

    public static PaxosAcceptorState getAcceptorStateForRound(long round) {
        return PaxosAcceptorState.newState(new PaxosProposalId(
                round, UUID.randomUUID().toString()));
    }

    public static byte[] longToBytes(long value) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        return buffer.array();
    }
}
