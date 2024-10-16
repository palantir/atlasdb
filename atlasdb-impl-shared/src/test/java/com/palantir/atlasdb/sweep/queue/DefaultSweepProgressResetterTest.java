/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class DefaultSweepProgressResetterTest {
    private static final TableReference BUCKET_PROGRESS_TABLE =
            TableReference.createFromFullyQualifiedName("test.bucketProgressTable");
    private static final TableReference SWEEP_BUCKET_ASSIGNED_TABLE =
            TableReference.createFromFullyQualifiedName("test.sweepBucketAssignedTable");

    private final AtomicInteger numberOfShardsProvider = new AtomicInteger(0);

    @Mock
    private KeyValueService keyValueService;

    @Mock
    private ShardProgress progress;

    private DefaultSweepProgressResetter sweepProgressResetter;

    @BeforeEach
    public void before() {
        sweepProgressResetter = new DefaultSweepProgressResetter(
                keyValueService,
                BUCKET_PROGRESS_TABLE,
                SWEEP_BUCKET_ASSIGNED_TABLE,
                progress,
                numberOfShardsProvider::get);
    }

    @Test
    public void truncatesBothSweepBucketAssignedAndBucketProgressTable() {
        sweepProgressResetter.resetProgress(Set.of());
        verify(keyValueService).truncateTables(Set.of(BUCKET_PROGRESS_TABLE, SWEEP_BUCKET_ASSIGNED_TABLE));
    }

    @Test
    public void resetsProgressForAllShardsAndStrategiesProvided() {
        numberOfShardsProvider.set(2);
        Set<SweeperStrategy> strategies = Set.of(SweeperStrategy.CONSERVATIVE, SweeperStrategy.THOROUGH);
        sweepProgressResetter.resetProgress(strategies);
        assertResetProgressForShard(2, strategies);
    }

    @Test
    public void resetsProgressForNewShardCountOnNextCall() {
        numberOfShardsProvider.set(2);
        Set<SweeperStrategy> strategies = Set.of(SweeperStrategy.CONSERVATIVE, SweeperStrategy.THOROUGH);
        sweepProgressResetter.resetProgress(strategies);
        assertResetProgressForShard(2, strategies);

        // Resetting the mock so that we can assert on the same values again when we rerun resetProgress
        reset(progress);

        numberOfShardsProvider.set(3);
        sweepProgressResetter.resetProgress(strategies);
        assertResetProgressForShard(3, strategies);
    }

    @Test
    public void resetsProgressForDifferentStrategiesProvided() {
        numberOfShardsProvider.set(2);
        Set<SweeperStrategy> strategies = Set.of(SweeperStrategy.CONSERVATIVE, SweeperStrategy.THOROUGH);
        sweepProgressResetter.resetProgress(strategies);
        assertResetProgressForShard(2, strategies);

        reset(progress);

        Set<SweeperStrategy> newStrategies = Set.of(SweeperStrategy.CONSERVATIVE, SweeperStrategy.NON_SWEEPABLE);
        sweepProgressResetter.resetProgress(newStrategies);
        assertResetProgressForShard(2, newStrategies);
    }

    private void assertResetProgressForShard(int numberOfShards, Set<SweeperStrategy> strategies) {
        for (int shard = 0; shard < numberOfShards; shard++) {
            for (SweeperStrategy strategy : strategies) {
                verify(progress).resetProgressForShard(ShardAndStrategy.of(shard, strategy));
            }
        }
    }
}
