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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class VerifyingPaxosStateLogTest {
    private static final long LEAST_LOG_ENTRY = 3L;
    private static final long GREATEST_LOG_ENTRY = 20L;
    private static final byte[] CORRECT_VALUE_FOR_ROUND = valueForRound(10L).persistToBytes();
    private static final byte[] OTHER_VALUE_FOR_ROUND = valueForRound(5L).persistToBytes();

    private final PaxosStateLog<PaxosValue> current = mock(PaxosStateLog.class);
    private final PaxosStateLog<PaxosValue> experimental = mock(PaxosStateLog.class);
    private final VerifyingPaxosStateLog.Settings<PaxosValue> settings = ImmutableSettings.<PaxosValue>builder()
            .currentLog(current)
            .experimentalLog(experimental)
            .hydrator(PaxosValue.BYTES_HYDRATOR)
            .build();

    private final PaxosStateLog<PaxosValue> log = new VerifyingPaxosStateLog<>(settings);

    @Before
    public void setupMocks() throws IOException {
        when(current.getLeastLogEntry()).thenReturn(LEAST_LOG_ENTRY);
        when(current.getGreatestLogEntry()).thenReturn(GREATEST_LOG_ENTRY);
        when(current.readRound(anyLong())).thenReturn(CORRECT_VALUE_FOR_ROUND);

        when(experimental.getLeastLogEntry()).thenReturn(LEAST_LOG_ENTRY + 1L);
        when(experimental.getGreatestLogEntry()).thenReturn(GREATEST_LOG_ENTRY + 1L);
        when(experimental.readRound(anyLong())).thenReturn(OTHER_VALUE_FOR_ROUND);
    }

    @After
    public void noMoreInteractions() {
        verifyNoMoreInteractions(current, experimental);
    }

    @Test
    public void writeWritesToBoth() {
        long sequence = 15L;
        log.writeRound(sequence, valueForRound(sequence));
        verify(current).writeRound(sequence, valueForRound(sequence));
        verify(experimental).writeRound(sequence, valueForRound(sequence));
    }

    @Test
    public void writeSuppressesExceptionInExperimental() {
        doThrow(new RuntimeException()).when(experimental).writeRound(anyLong(), any(PaxosValue.class));
        long sequence = 15L;
        assertThatCode(() -> log.writeRound(sequence, valueForRound(sequence))).doesNotThrowAnyException();
        verify(current).writeRound(sequence, valueForRound(sequence));
        verify(experimental).writeRound(sequence, valueForRound(sequence));
    }

    @Test
    public void readFromBothReturnLegacy() throws IOException {
        long sequence = 10L;
        assertThat(log.readRound(sequence)).isEqualTo(CORRECT_VALUE_FOR_ROUND);
        verify(current).readRound(sequence);
        verify(experimental).readRound(sequence);
    }

    @Test
    public void readSuppressesExceptionInExperimental() throws IOException {
        when(experimental.readRound(anyLong())).thenThrow(new RuntimeException());
        long sequence = 10L;
        assertThatCode(() -> log.readRound(sequence)).doesNotThrowAnyException();
        verify(current).readRound(sequence);
        verify(experimental).readRound(sequence);
    }

    @Test
    public void getLeastFromBothReturnLegacy() {
        assertThat(log.getLeastLogEntry()).isEqualTo(LEAST_LOG_ENTRY);
        verify(current).getLeastLogEntry();
        verify(experimental).getLeastLogEntry();
    }

    @Test
    public void getLeastSuppressesExceptionInExperimental() {
        when(experimental.getLeastLogEntry()).thenThrow(new RuntimeException());
        assertThatCode(log::getLeastLogEntry).doesNotThrowAnyException();
        verify(current).getLeastLogEntry();
        verify(experimental).getLeastLogEntry();
    }

    @Test
    public void getGreatestFromBothReturnLegacy() {
        assertThat(log.getGreatestLogEntry()).isEqualTo(GREATEST_LOG_ENTRY);
        verify(current).getGreatestLogEntry();
        verify(experimental).getGreatestLogEntry();
    }

    @Test
    public void getGreatestSuppressesExceptionInExperimental() {
        when(experimental.getGreatestLogEntry()).thenThrow(new RuntimeException());
        assertThatCode(log::getGreatestLogEntry).doesNotThrowAnyException();
        verify(current).getGreatestLogEntry();
        verify(experimental).getGreatestLogEntry();
    }

    @Test
    public void truncateBoth() {
        log.truncate(7L);
        verify(current).truncate(7L);
        verify(experimental).truncate(7L);
    }

    @Test
    public void truncateFailsOnExceptionInExperimentalAndIsNotCalledOnCurrent() {
        doThrow(new RuntimeException()).when(experimental).truncate(anyLong());

        assertThatThrownBy(() -> log.truncate(7L)).isInstanceOf(RuntimeException.class);
        verify(experimental).truncate(7L);
        verify(current, never()).truncate(anyLong());
    }

    private static PaxosValue valueForRound(long round) {
        return new PaxosValue("someLeader", round, longToBytes(round));
    }

    private static byte[] longToBytes(long value) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        return buffer.array();
    }
}
