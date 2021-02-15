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

package com.palantir.paxos;

import com.palantir.common.base.Throwables;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public final class PaxosStateLogTestUtils {
    public static final NamespaceAndUseCase NAMESPACE = wrap(Client.of("client"), "tom");

    private PaxosStateLogTestUtils() {
        // adsfg
    }

    public static NamespaceAndUseCase wrap(Client namespace, String useCase) {
        return ImmutableNamespaceAndUseCase.of(namespace, useCase);
    }

    public static List<PaxosRound<PaxosValue>> generateRounds(LongStream longStream) {
        return longStream
                .mapToObj(PaxosStateLogTestUtils::valueForRound)
                .map(paxosValue -> PaxosRound.of(paxosValue.seq, paxosValue))
                .collect(Collectors.toList());
    }

    public static PaxosValue valueForRound(long round) {
        return new PaxosValue("someLeader", round, longToBytes(round));
    }

    public static byte[] longToBytes(long value) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        return buffer.array();
    }

    public static PaxosValue getPaxosValue(PaxosStateLog<PaxosValue> log, long seq) {
        return PaxosValue.BYTES_HYDRATOR.hydrateFromBytes(readRoundUnchecked(log, seq));
    }

    public static byte[] readRoundUnchecked(PaxosStateLog<?> log, long seq) {
        try {
            return log.readRound(seq);
        } catch (IOException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }
}
