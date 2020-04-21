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

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class MigrationPaxosStateLogTest {
    private final PaxosStateLog<PaxosValue> legacy = mock(PaxosStateLog.class);
    private final PaxosStateLog<PaxosValue> current = mock(PaxosStateLog.class);

    @Test
    public void migrationIsPerformedWhenStateHasNoCorruption() throws IOException {
        when(legacy.getLeastLogEntry()).thenReturn(3L);
        when(legacy.getGreatestLogEntry()).thenReturn(5L);
        when(legacy.readRound(anyLong()))
                .thenAnswer(invocation -> valueForRound(invocation.getArgument(0)).persistToBytes());

        MigrationPaxosStateLog.create(legacy, current, PaxosValue.BYTES_HYDRATOR);

        for (long sequence = 3L; sequence <= 5L; sequence++) {
            verify(current).writeRound(sequence, valueForRound(sequence));
        }
        verifyNoMoreInteractions(current);
    }

    @Test
    public void migrationSkipsNonExistentEntries() throws IOException {
        when(legacy.getLeastLogEntry()).thenReturn(3L);
        when(legacy.getGreatestLogEntry()).thenReturn(5L);
        when(legacy.readRound(anyLong()))
                .thenAnswer(invocation -> {
                    long sequence = invocation.getArgument(0);
                    return sequence == 4 ? null : valueForRound(invocation.getArgument(0)).persistToBytes();
                });

        MigrationPaxosStateLog.create(legacy, current, PaxosValue.BYTES_HYDRATOR);

        verify(current).writeRound(3L, valueForRound(3L));
        verify(current).writeRound(5L, valueForRound(5L));
        verifyNoMoreInteractions(current);
    }

    @Test
    public void migrationFailsIfCorruptionIsDetected() throws IOException {
        when(legacy.getLeastLogEntry()).thenReturn(3L);
        when(legacy.getGreatestLogEntry()).thenReturn(5L);
        when(legacy.readRound(3L)).thenReturn(valueForRound(3L).persistToBytes());
        PaxosStateLog.CorruptLogFileException cause = new PaxosStateLog.CorruptLogFileException();
        doThrow(cause).when(legacy).readRound(4L);
        Assertions.assertThatThrownBy(() -> MigrationPaxosStateLog.create(legacy, current, PaxosValue.BYTES_HYDRATOR))
                .isInstanceOf(RuntimeException.class)
                .hasCause(cause);
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
