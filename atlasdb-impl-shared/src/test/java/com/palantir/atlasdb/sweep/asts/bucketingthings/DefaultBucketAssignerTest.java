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

package com.palantir.atlasdb.sweep.asts.bucketingthings;

import static org.mockito.Mockito.when;

import com.palantir.atlasdb.cleaner.PuncherStore;
import com.palantir.atlasdb.sweep.queue.ShardProgress;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

public final class DefaultBucketAssignerTest {
    @Mock
    private ShardProgress shardProgress;

    @Mock
    private SweepBucketsTable sweepBucketsTable;

    @Mock
    private SweepBucketWritePointerTable sweepBucketWritePointerTable;

    @Mock
    private PuncherStore puncherStore;

    @Mock
    private SweeperStrategy sweeperStrategy;

    @Mock
    private Consumer<Long> newBucketIdentifierReporter;

    private Runnable bucketAssigner;

    @BeforeEach
    public void beforeEach() {
        bucketAssigner = DefaultBucketAssigner.create(
                shardProgress,
                sweepBucketsTable,
                sweepBucketWritePointerTable,
                puncherStore,
                sweeperStrategy,
                newBucketIdentifierReporter);
    }

    @Test
    public void bucketIdentifiersAlwaysSequential() {}

    @Test
    public void bucketSizeApproximatelyTenMinutesUsingPunchTable() {}

    @Test
    public void oneBucketForEachShardCreated() {}

    @Test
    public void abortedAttemptsCanBeCompleted() {}

    @Test
    public void abortsIfContendingWithAnotherAssigner() {}

    @Test
    public void createsBucketWithConsistentOrder() {}

    @Test
    public void updatesHighestBucketNumberAfterClosingBucket() {}

    @Test
    public void retryingAfterFailingToUpdateLastTimestampForBucketDoesNotOverwriteExistingBucket() {}

    @Test
    public void createsBucketsUntilCurrentOpenBucketIfLessThanMaxBuckets() {}

    @Test
    public void stopsCreatingBucketsInSingleRunAfterCreatingMaxBuckets() {}

    @Test
    public void retryingAfterFailingToUpdateHighestBucketNumberOverwritesExistingBucketsWithOriginalStartTimestamp() {
        // e.g.
    }

    @Test
    public void canOverwriteExistingBucketIfLastTimestampForBucketIsUpdatedAndBucketNumberNotIncreasedPassed() {}

    private void setupMocks(
            int numberOfShards,
            long currentBucketNumber,
            long lastTimestampForBucket,
            long lastWallclockTimeForBucket) {
        when(shardProgress.getNumberOfShards()).thenReturn(numberOfShards);
        when(sweepBucketWritePointerTable.getHighestBucketNumber()).thenReturn(currentBucketNumber);
        when(sweepBucketWritePointerTable.getLastTimestampForBucket()).thenReturn(lastTimestampForBucket);
        when(puncherStore.getMillisForTimestamp(lastTimestampForBucket)).thenReturn(lastWallclockTimeForBucket);
    }
}
