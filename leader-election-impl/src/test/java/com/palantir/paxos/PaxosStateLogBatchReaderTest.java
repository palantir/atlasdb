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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.junit.Test;

import com.google.common.util.concurrent.Uninterruptibles;

public class PaxosStateLogBatchReaderTest {
    private static final int START_SEQUENCE = 123;
    private static final int BATCH_SIZE = 250;
    private static final List<PaxosRound<PaxosValue>> EXPECTED_ROUNDS = LongStream
            .range(START_SEQUENCE, START_SEQUENCE + BATCH_SIZE)
            .mapToObj(PaxosStateLogBatchReaderTest::valueForRound)
            .map(value -> PaxosRound.of(value.seq, value))
            .collect(Collectors.toList());

    private PaxosStateLog<PaxosValue> mockLog = mock(PaxosStateLog.class);

    @Test
    public void readConsecutiveBatch() throws IOException {
        when(mockLog.readRound(anyLong()))
                .thenAnswer(invocation -> valueForRound((long) invocation.getArguments()[0]).persistToBytes());

        try (PaxosStateLogBatchReader<PaxosValue> reader = createReader()) {
            assertThat(reader.readBatch(START_SEQUENCE, BATCH_SIZE)).isEqualTo(EXPECTED_ROUNDS);
        }
    }

    @Test
    public void exceptionsArePropagated() throws IOException {
        IOException ioException = new IOException("test");
        when(mockLog.readRound(anyLong()))
                .thenAnswer(invocation -> {
                    long sequence = (long) invocation.getArguments()[0];
                    if (sequence == 200) {
                        throw ioException;
                    }
                    return valueForRound(sequence).persistToBytes();
                });

        try (PaxosStateLogBatchReader<PaxosValue> reader = createReader()) {
            assertThatThrownBy(() -> reader.readBatch(START_SEQUENCE, BATCH_SIZE)).isInstanceOf(RuntimeException.class);
        }
    }

    @Test
    public void readBatchFiltersOutNulls() throws IOException {
        when(mockLog.readRound(anyLong()))
                .thenAnswer(invocation -> {
                    long sequence = (long) invocation.getArguments()[0];
                    if (sequence % 2 == 0) {
                        return null;
                    }
                    return valueForRound(sequence).persistToBytes();
                });

        try (PaxosStateLogBatchReader<PaxosValue> reader = createReader()) {
            assertThat(reader.readBatch(START_SEQUENCE, BATCH_SIZE))
                    .isEqualTo(EXPECTED_ROUNDS.stream()
                            .filter(round -> round.sequence() % 2 != 0)
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
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            return valueForRound((long) invocation.getArguments()[0]).persistToBytes();
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

    private static PaxosValue valueForRound(long round) {
        byte[] bytes = new byte[] { 1, 2, 3 };
        return new PaxosValue("someLeader", round, bytes);
    }
}
