/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.junit.Test;

public class SweepBatchWithPartitionInfoTest {
    private final SweepBatch batch = mock(SweepBatch.class);

    @Test
    public void sweepWithinSinglePartitionLeadsToNoPartitionsToClear() {
        when(batch.lastSweptTimestamp()).thenReturn(SweepQueueUtils.minTsForFinePartition(19) + 84L);
        SweepBatchWithPartitionInfo batchWithPartitionInfo
                = SweepBatchWithPartitionInfo.of(batch, ImmutableSet.of(19L));
        assertThat(batchWithPartitionInfo.partitionsForPreviousLastSweptTs(SweepQueueUtils.minTsForFinePartition(19)))
                .containsExactlyInAnyOrder();
    }

    @Test
    public void currentlyActivePartitionIsNotCleared() {
        when(batch.lastSweptTimestamp()).thenReturn(SweepQueueUtils.minTsForFinePartition(19) + 84L);
        SweepBatchWithPartitionInfo batchWithPartitionInfo
                = SweepBatchWithPartitionInfo.of(batch, ImmutableSet.of(15L, 16L, 17L, 18L, 19L));
        assertThat(batchWithPartitionInfo.partitionsForPreviousLastSweptTs(SweepQueueUtils.INITIAL_TIMESTAMP))
                .containsExactlyInAnyOrder(15L, 16L, 17L, 18L);
    }

    @Test
    public void partitionIsClearableIfWeHaveSweptTillItsEnd() {
        when(batch.lastSweptTimestamp()).thenReturn(SweepQueueUtils.maxTsForFinePartition(19));
        SweepBatchWithPartitionInfo batchWithPartitionInfo
                = SweepBatchWithPartitionInfo.of(batch, ImmutableSet.of(19L));
        assertThat(batchWithPartitionInfo.partitionsForPreviousLastSweptTs(SweepQueueUtils.INITIAL_TIMESTAMP))
                .containsExactlyInAnyOrder(19L);
    }

    @Test
    public void partitionsNeedNotBeContiguous() {
        when(batch.lastSweptTimestamp()).thenReturn(SweepQueueUtils.maxTsForFinePartition(19));
        Set<Long> partitions = ImmutableSet.of(3L, 9L, 12L, 15L, 17L);
        SweepBatchWithPartitionInfo batchWithPartitionInfo = SweepBatchWithPartitionInfo.of(batch, partitions);
        assertThat(batchWithPartitionInfo.partitionsForPreviousLastSweptTs(SweepQueueUtils.INITIAL_TIMESTAMP))
                .hasSameElementsAs(partitions);
    }

    @Test
    public void cleansUpOldRowIfNeeded() {
        when(batch.lastSweptTimestamp()).thenReturn(SweepQueueUtils.maxTsForFinePartition(19));
        SweepBatchWithPartitionInfo batchWithPartitionInfo
                = SweepBatchWithPartitionInfo.of(batch, ImmutableSet.of(19L));
        assertThat(batchWithPartitionInfo.partitionsForPreviousLastSweptTs(SweepQueueUtils.minTsForFinePartition(8L)))
                .containsExactlyInAnyOrder(8L, 19L);
    }

    @Test
    public void cleansUpOldRowEvenIfItHasReachedItsEnd() { // Needed for legacy compatibility
        when(batch.lastSweptTimestamp()).thenReturn(SweepQueueUtils.maxTsForFinePartition(19));
        SweepBatchWithPartitionInfo batchWithPartitionInfo
                = SweepBatchWithPartitionInfo.of(batch, ImmutableSet.of(19L));
        assertThat(batchWithPartitionInfo.partitionsForPreviousLastSweptTs(SweepQueueUtils.maxTsForFinePartition(8L)))
                .containsExactlyInAnyOrder(8L, 19L);
    }
}
