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

package com.palantir.atlasdb.timelock.transaction.timestamp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.timelock.transaction.client.NumericPartitionAllocator;
import com.palantir.lock.v2.TimestampAndPartition;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.timestamp.TimestampRange;
import java.util.List;
import java.util.UUID;
import org.junit.After;
import org.junit.Test;

@SuppressWarnings("unchecked") // Mocks of parameterised types
public class DelegatingClientAwareManagedTimestampServiceTest {
    private static final List<Integer> RESIDUE_ONE = ImmutableList.of(1);
    private static final List<Integer> RESIDUE_TWO = ImmutableList.of(2);

    private static final UUID UUID_ONE = UUID.randomUUID();
    private static final UUID UUID_TWO = UUID.randomUUID();

    private static final TimestampRange TIMESTAMP_RANGE = TimestampRange.createInclusiveRange(
            DelegatingClientAwareManagedTimestampService.NUM_PARTITIONS,
            DelegatingClientAwareManagedTimestampService.NUM_PARTITIONS + 3);
    private static final TimestampAndPartition RESIDUE_ONE_TIMESTAMP_IN_RANGE
            = TimestampAndPartition.of(DelegatingClientAwareManagedTimestampService.NUM_PARTITIONS + 1, 1);
    private static final TimestampAndPartition RESIDUE_TWO_TIMESTAMP_IN_RANGE
            = TimestampAndPartition.of(DelegatingClientAwareManagedTimestampService.NUM_PARTITIONS + 2, 2);

    private static final TimestampRange TIMESTAMP_SEVEN = TimestampRange.createInclusiveRange(7, 7);

    private final NumericPartitionAllocator<UUID> allocator = mock(NumericPartitionAllocator.class);
    private final ManagedTimestampService timestamps = mock(ManagedTimestampService.class);
    private final DelegatingClientAwareManagedTimestampService service
            = new DelegatingClientAwareManagedTimestampService(allocator, timestamps);

    @After
    public void verifyNoFurtherInteractions() {
        verifyNoMoreInteractions(timestamps, allocator);
    }

    @Test
    public void getsFreshTimestampsFromDelegate() {
        service.getFreshTimestamp();
        verify(timestamps).getFreshTimestamp();
    }

    @Test
    public void getsFreshTimestampsMultipleFromDelegate() {
        service.getFreshTimestamps(77);
        verify(timestamps).getFreshTimestamps(eq(77));
    }

    @Test
    public void pingsDelegate() {
        service.ping();
        verify(timestamps).ping();
    }

    @Test
    public void fastForwardsDelegate() {
        service.fastForwardTimestamp(12345);
        verify(timestamps).fastForwardTimestamp(12345);
    }

    @Test
    public void batchedTimestampCallShouldReturnAtLeastOneUsableTimestamp() {
        when(allocator.getRelevantModuli(UUID_ONE)).thenReturn(RESIDUE_ONE);
        when(timestamps.getFreshTimestamps(anyInt()))
                .thenReturn(TIMESTAMP_RANGE);

        assertThat(service.getFreshTimestampsForClient(UUID_ONE, 2).stream())
                .containsExactly(RESIDUE_ONE_TIMESTAMP_IN_RANGE.timestamp());

        verify(allocator).getRelevantModuli(UUID_ONE);
        verify(timestamps).getFreshTimestamps(anyInt());
    }

    @Test
    public void batchedTimestampCallMakesRequestsAgainIfTimestampRangeDoesNotIncludeCorrectModuli() {
        when(allocator.getRelevantModuli(UUID_TWO)).thenReturn(RESIDUE_TWO);
        when(timestamps.getFreshTimestamps(anyInt()))
                .thenReturn(TIMESTAMP_SEVEN)
                .thenReturn(TIMESTAMP_SEVEN)
                .thenReturn(TIMESTAMP_RANGE);

        assertThat(service.getFreshTimestampsForClient(UUID_TWO, 1).stream())
                .containsExactly(RESIDUE_TWO_TIMESTAMP_IN_RANGE.timestamp());

        verify(allocator, times(3)).getRelevantModuli(UUID_TWO);
        verify(timestamps, times(3)).getFreshTimestamps(anyInt());
    }
}
