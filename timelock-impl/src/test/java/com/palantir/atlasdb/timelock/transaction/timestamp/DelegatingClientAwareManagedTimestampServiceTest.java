/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
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

import java.util.List;
import java.util.UUID;

import org.junit.After;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.timelock.paxos.ManagedTimestampService;
import com.palantir.atlasdb.timelock.transaction.client.NumericPartitionAllocator;
import com.palantir.timestamp.TimestampRange;

@SuppressWarnings("unchecked") // Mocks of parameterised types
public class DelegatingClientAwareManagedTimestampServiceTest {
    private static final List<Integer> MODULUS_ONE = ImmutableList.of(1);
    private static final List<Integer> MODULUS_TWO = ImmutableList.of(2);

    private static final UUID UUID_ONE = UUID.randomUUID();
    private static final UUID UUID_TWO = UUID.randomUUID();

    private static final TimestampRange TIMESTAMP_RANGE = TimestampRange.createInclusiveRange(
            DelegatingClientAwareManagedTimestampService.NUM_PARTITIONS,
            DelegatingClientAwareManagedTimestampService.NUM_PARTITIONS + 2);
    private static final long MODULUS_ONE_TIMESTAMP_IN_RANGE
            = DelegatingClientAwareManagedTimestampService.NUM_PARTITIONS + 1;
    private static final TimestampRange TIMESTAMP_ZERO = TimestampRange.createInclusiveRange(0, 0);

    private NumericPartitionAllocator<UUID> allocator = mock(NumericPartitionAllocator.class);
    private ManagedTimestampService timestamps = mock(ManagedTimestampService.class);
    private DelegatingClientAwareManagedTimestampService service = new DelegatingClientAwareManagedTimestampService(
            allocator, timestamps);

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
    public void delegatesRequestToAllocator() {
        when(allocator.getRelevantModuli(UUID_ONE)).thenReturn(MODULUS_ONE);
        when(allocator.getRelevantModuli(UUID_TWO)).thenReturn(MODULUS_TWO);
        when(timestamps.getFreshTimestamps(anyInt())).thenReturn(TIMESTAMP_RANGE);

        assertThat(service.getFreshTimestampForClient(UUID_ONE)).isEqualTo(MODULUS_ONE_TIMESTAMP_IN_RANGE);

        verify(allocator).getRelevantModuli(UUID_ONE);
        verify(timestamps).getFreshTimestamps(anyInt());
    }

    @Test
    public void makesRequestsAgainIfTimestampRangeDoesNotIncludeCorrectModuli() {
        when(allocator.getRelevantModuli(UUID_ONE)).thenReturn(MODULUS_ONE);
        when(allocator.getRelevantModuli(UUID_TWO)).thenReturn(MODULUS_TWO);
        when(timestamps.getFreshTimestamps(anyInt()))
                .thenReturn(TIMESTAMP_ZERO)
                .thenReturn(TIMESTAMP_ZERO)
                .thenReturn(TIMESTAMP_RANGE);

        assertThat(service.getFreshTimestampForClient(UUID_ONE)).isEqualTo(MODULUS_ONE_TIMESTAMP_IN_RANGE);

        verify(allocator, times(3)).getRelevantModuli(UUID_ONE);
        verify(timestamps, times(3)).getFreshTimestamps(anyInt());

    }
}
