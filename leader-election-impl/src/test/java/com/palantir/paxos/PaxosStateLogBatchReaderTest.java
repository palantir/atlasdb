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

import static com.palantir.paxos.PaxosStateLogTestUtils.generateRounds;
import static com.palantir.paxos.PaxosStateLogTestUtils.valueForRound;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.Test;

public class PaxosStateLogBatchReaderTest {
    private static final int START_SEQUENCE = 123;
    private static final int BATCH_SIZE = 250;
    private static final List<PaxosRound<PaxosValue>> EXPECTED_ROUNDS =
            generateRounds(LongStream.range(START_SEQUENCE, START_SEQUENCE + BATCH_SIZE));

    private PaxosStateLog<PaxosValue> mockLog = mock(PaxosStateLog.class);

    @Test
    public void readConsecutiveBatch() throws IOException {
        when(mockLog.readRound(anyLong())).thenAnswer(invocation -> valueForRound(invocation.getArgument(0))
                .persistToBytes());

        try (PaxosStateLogBatchReader<PaxosValue> reader = createReader()) {
            assertThat(reader.readBatch(START_SEQUENCE, BATCH_SIZE)).isEqualTo(EXPECTED_ROUNDS);
        }
    }

    @Test
    public void exceptionsArePropagated() throws IOException {
        IOException ioException = new IOException("test");
        when(mockLog.readRound(anyLong())).thenAnswer(invocation -> {
            long sequence = invocation.getArgument(0);
            if (sequence == 200) {
                throw ioException;
            }
            return valueForRound(sequence).persistToBytes();
        });

        try (PaxosStateLogBatchReader<PaxosValue> reader = createReader()) {
            assertThatThrownBy(() -> reader.readBatch(START_SEQUENCE, BATCH_SIZE))
                    .isInstanceOf(RuntimeException.class);
        }
    }

    @Test
    public void readBatchFiltersOutNulls() throws IOException {
        Predicate<Long> isOdd = num -> num % 2 != 0;
        when(mockLog.readRound(anyLong())).thenAnswer(invocation -> {
            long sequence = invocation.getArgument(0);
            if (!isOdd.test(sequence)) {
                return null;
            }
            return valueForRound(sequence).persistToBytes();
        });

        try (PaxosStateLogBatchReader<PaxosValue> reader = createReader()) {
            assertThat(reader.readBatch(START_SEQUENCE, BATCH_SIZE))
                    .isEqualTo(EXPECTED_ROUNDS.stream()
                            .filter(round -> isOdd.test(round.sequence()))
                            .collect(Collectors.toList()));
        }
    }

    @Test
    public void noResultsReturnsEmptyList() throws IOException {
        when(mockLog.readRound(anyLong())).thenReturn(null);

        try (PaxosStateLogBatchReader<PaxosValue> reader = createReader()) {
            assertThat(reader.readBatch(START_SEQUENCE, BATCH_SIZE)).isEmpty();
        }
    }

    @Test
    public void executionsGetBatched() throws IOException {
        when(mockLog.readRound(anyLong())).thenAnswer(invocation -> {
            Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(100));
            return valueForRound(invocation.getArgument(0)).persistToBytes();
        });

        try (PaxosStateLogBatchReader<PaxosValue> reader = createReader()) {
            Instant startInstant = Instant.now();
            reader.readBatch(START_SEQUENCE, BATCH_SIZE);
            assertThat(Duration.between(Instant.now(), startInstant)).isLessThan(Duration.ofSeconds(1));
        }
    }

    private PaxosStateLogBatchReader<PaxosValue> createReader() {
        return new PaxosStateLogBatchReader<>(mockLog, PaxosValue.BYTES_HYDRATOR, 100);
    }
}
