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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;

public class MigrationPaxosStateLogTest {
    private final PaxosStateLog<PaxosValue> legacy = mock(PaxosStateLog.class);
    private final PaxosStateLog<PaxosValue> current = mock(PaxosStateLog.class);
    private final MigrationPaxosStateLog.Settings<PaxosValue> settings = ImmutableSettings.<PaxosValue>builder()
            .sourceOfTruth(legacy)
            .secondary(current)
            .hydrator(PaxosValue.BYTES_HYDRATOR)
            .build();

    @Before
    public void setupMocks() throws IOException {
        when(legacy.getLeastLogEntry()).thenReturn(3L);
        when(legacy.getGreatestLogEntry()).thenReturn(5L);
        when(legacy.readRound(anyLong()))
                .thenAnswer(invocation -> valueForRound(invocation.getArgument(0)).persistToBytes());
        when(current.getLeastLogEntry()).thenReturn(4L);
        when(current.getGreatestLogEntry()).thenReturn(6L);
        when(current.readRound(anyLong())).thenReturn(valueForRound(10L).persistToBytes());
    }

    @Test
    public void migrationIsPerformedWhenStateHasNoCorruption() {
        MigrationPaxosStateLog.create(settings);

        for (long sequence = 3L; sequence <= 5L; sequence++) {
            verify(current).writeRound(sequence, valueForRound(sequence));
        }
        verifyNoMoreInteractions(current);
    }

    @Test
    public void migrationSkipsNonExistentEntries() throws IOException {
        when(legacy.readRound(4L)).thenReturn(null);

        MigrationPaxosStateLog.create(settings);

        verify(current).writeRound(3L, valueForRound(3L));
        verify(current).writeRound(5L, valueForRound(5L));
        verifyNoMoreInteractions(current);
    }

    @Test
    public void migrationFailsIfCorruptionIsDetected() throws IOException {
        PaxosStateLog.CorruptLogFileException cause = new PaxosStateLog.CorruptLogFileException();
        doThrow(cause).when(legacy).readRound(4L);

        assertThatThrownBy(() -> MigrationPaxosStateLog.create(settings))
                .isInstanceOf(RuntimeException.class)
                .hasCause(cause);
    }

    @Test
    public void writeWritesToBoth() {
        PaxosStateLog<PaxosValue> log = MigrationPaxosStateLog.create(settings);
        log.writeRound(15L, valueForRound(15));
        verify(legacy).writeRound(15L, valueForRound(15L));
        verify(current).writeRound(15L, valueForRound(15L));
    }

    @Test
    public void readFromBothReturnLegacy() throws IOException {
        PaxosStateLog<PaxosValue> log = MigrationPaxosStateLog.create(settings);
        long sequence = 15L;
        assertThat(log.readRound(sequence)).isEqualTo(valueForRound(sequence).persistToBytes());
        verify(legacy).readRound(sequence);
        verify(current).readRound(sequence);
        assertThat(legacy.readRound(sequence)).isNotEqualTo(current.readRound(sequence));
    }

    @Test
    public void getLeastFromBothReturnLegacy() {
        PaxosStateLog<PaxosValue> log = MigrationPaxosStateLog.create(settings);
        assertThat(log.getLeastLogEntry()).isEqualTo(3L);
        verify(legacy, atLeastOnce()).getLeastLogEntry();
        verify(current).getLeastLogEntry();
        assertThat(legacy.getLeastLogEntry()).isNotEqualTo(current.getLeastLogEntry());
    }

    @Test
    public void getGreatestFromBothReturnLegacy() {
        PaxosStateLog<PaxosValue> log = MigrationPaxosStateLog.create(settings);
        assertThat(log.getGreatestLogEntry()).isEqualTo(5L);
        verify(legacy, atLeastOnce()).getGreatestLogEntry();
        verify(current).getGreatestLogEntry();
        assertThat(legacy.getGreatestLogEntry()).isNotEqualTo(current.getGreatestLogEntry());
    }

    @Test
    public void truncateBoth() {
        PaxosStateLog<PaxosValue> log = MigrationPaxosStateLog.create(settings);
        log.truncate(7L);
        verify(legacy).truncate(7L);
        verify(current).truncate(7L);
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
