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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.Set;
import org.immutables.value.Value;

@Value.Immutable
public interface SweepBatchWithPartitionInfo {
    SweepBatch sweepBatch();

    Set<Long> finePartitions();

    default Set<Long> partitionsForPreviousLastSweptTs(long previousLastSweptTs) {
        Set<Long> encounteredPartitions = SweepQueueUtils.firstSweep(previousLastSweptTs)
                ? finePartitions()
                : Sets.union(finePartitions(), ImmutableSet.of(SweepQueueUtils.tsPartitionFine(previousLastSweptTs)));

        return Sets.difference(
                encounteredPartitions,
                ImmutableSet.of(SweepQueueUtils.tsPartitionFine(sweepBatch().lastSweptTimestamp() + 1)));
    }

    static SweepBatchWithPartitionInfo of(SweepBatch sweepBatch, Set<Long> finePartitions) {
        return ImmutableSweepBatchWithPartitionInfo.builder()
                .sweepBatch(sweepBatch)
                .finePartitions(finePartitions)
                .build();
    }
}
