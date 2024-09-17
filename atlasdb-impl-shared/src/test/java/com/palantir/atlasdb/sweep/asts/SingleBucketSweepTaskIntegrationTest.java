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

package com.palantir.atlasdb.sweep.asts;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.sweep.asts.SweepableBucket.TimestampRange;
import com.palantir.atlasdb.sweep.asts.bucketingthings.BucketAssignerState;
import com.palantir.atlasdb.sweep.asts.bucketingthings.SweepBucketAssignerStateMachineTable;
import com.palantir.atlasdb.sweep.asts.bucketingthings.SweepBucketAssignerStateMachineTable.BucketStateAndIdentifier;
import com.palantir.atlasdb.sweep.asts.bucketingthings.SweepBucketsTable;
import com.palantir.atlasdb.sweep.asts.progress.BucketProgress;
import com.palantir.atlasdb.sweep.asts.progress.BucketProgressStore;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetricsConfigurations;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.SweepQueueDeleter;
import com.palantir.atlasdb.sweep.queue.SweepQueueReader;
import com.palantir.atlasdb.sweep.queue.SweepQueueUtils;
import com.palantir.atlasdb.sweep.queue.SweepQueueWriter;
import com.palantir.atlasdb.sweep.queue.SweepableCells;
import com.palantir.atlasdb.sweep.queue.SweepableTimestamps;
import com.palantir.atlasdb.sweep.queue.TargetedSweepFollower;
import com.palantir.atlasdb.sweep.queue.WriteInfo;
import com.palantir.atlasdb.sweep.queue.WriteInfoPartitioner;
import com.palantir.atlasdb.sweep.queue.clear.DefaultTableClearer;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.service.SimpleTransactionService;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SingleBucketSweepTaskIntegrationTest {
    private static final int SHARDS = 1;

    private static final TableReference CONSERVATIVE_TABLE = TableReference.createFromFullyQualifiedName("terri.tory");
    private static final Bucket CONSERVATIVE_BUCKET_ZERO = Bucket.of(ShardAndStrategy.conservative(0), 0);
    private static final long END_OF_BUCKET_ZERO = 1000L;
    private static final TimestampRange BUCKET_ZERO_TIMESTAMP_RANGE = TimestampRange.of(0L, END_OF_BUCKET_ZERO);
    private static final SweepableBucket SWEEPABLE_CONSERVATIVE_BUCKET_ZERO =
            SweepableBucket.of(CONSERVATIVE_BUCKET_ZERO, BUCKET_ZERO_TIMESTAMP_RANGE);
    private static final Bucket CONSERVATIVE_BUCKET_ONE = Bucket.of(ShardAndStrategy.conservative(0), 1);
    private static final SweepableBucket SWEEPABLE_CONSERVATIVE_BUCKET_ONE =
            SweepableBucket.of(CONSERVATIVE_BUCKET_ONE, TimestampRange.of(END_OF_BUCKET_ZERO, 5000L));
    private static final Bucket CONSERVATIVE_BUCKET_TWO = Bucket.of(ShardAndStrategy.conservative(0), 2);
    private static final SweepableBucket SWEEPABLE_CONSERVATIVE_BUCKET_TWO =
            SweepableBucket.of(CONSERVATIVE_BUCKET_TWO, TimestampRange.of(5000L, -1)); // open bucket
    private static final Cell DEFAULT_CELL = Cell.create(PtBytes.toBytes("row"), PtBytes.toBytes("column"));
    private static final byte[] DEFAULT_VALUE = PtBytes.toBytes("value");
    private static final byte[] ANOTHER_VALUE = PtBytes.toBytes("another");
    private static final byte[] THIRD_VALUE = PtBytes.toBytes("drittel");

    private final KeyValueService keyValueService = new InMemoryKeyValueService(true);
    private final WriteInfoPartitioner writeInfoPartitioner = new WriteInfoPartitioner(keyValueService, () -> SHARDS);
    private final MetricsManager metricsManager = MetricsManagers.createForTests();
    private final TransactionService transactionService =
            SimpleTransactionService.createV3(keyValueService, metricsManager.getTaggedRegistry(), () -> false);

    private final TargetedSweepMetrics targetedSweepMetrics = TargetedSweepMetrics.createWithClock(
            metricsManager, keyValueService, mock(Clock.class), TargetedSweepMetricsConfigurations.DEFAULT, SHARDS);
    private final SweepableTimestamps sweepableTimestamps =
            new SweepableTimestamps(keyValueService, writeInfoPartitioner);
    private final SweepableCells sweepableCells =
            new SweepableCells(keyValueService, writeInfoPartitioner, targetedSweepMetrics, transactionService);
    private final SweepQueueReader sweepQueueReader = new SweepQueueReader(
            sweepableTimestamps, sweepableCells, SweepQueueReader.DEFAULT_READ_BATCHING_RUNTIME_CONTEXT);
    private final AtomicLong sweepTimestamp = new AtomicLong(0);
    private final SweepQueueDeleter sweepQueueDeleter = new SweepQueueDeleter(
            keyValueService,
            mock(TargetedSweepFollower.class), // Not important for this test
            new DefaultTableClearer(keyValueService, sweepTimestamp::get),
            _unused -> Optional.empty());
    private final SweepQueueWriter sweepQueueWriter =
            new SweepQueueWriter(sweepableTimestamps, sweepableCells, writeInfoPartitioner);

    private final BucketProgressStore bucketProgressStore = new TestBucketProgressStore();

    private SweepBucketsTable sweepBucketsTable;
    private SweepBucketAssignerStateMachineTable sweepBucketAssignerStateMachineTable;

    private SingleBucketSweepTask singleBucketSweepTask;

    @BeforeEach
    public void setUp() {
        sweepBucketsTable = mock(SweepBucketsTable.class);
        sweepBucketAssignerStateMachineTable = mock(SweepBucketAssignerStateMachineTable.class);
        singleBucketSweepTask = new DefaultSingleBucketSweepTask(
                bucketProgressStore,
                sweepQueueReader,
                sweepQueueDeleter,
                sweepTimestamp::get,
                targetedSweepMetrics,
                sweepBucketsTable,
                sweepBucketAssignerStateMachineTable);

        keyValueService.createTable(
                CONSERVATIVE_TABLE,
                TableMetadata.builder()
                        .sweepStrategy(SweepStrategy.CONSERVATIVE)
                        .build()
                        .persistToBytes());
    }

    // [vw       x][          ]
    //      ^ Sweep Timestamp
    //      ^ Sweep Task
    @Test
    public void sweepsFromClosedBucketUpToSweepTimestamp() {
        writeCell(100L, DEFAULT_VALUE, 200L);
        writeCell(300L, ANOTHER_VALUE, 400L);
        writeCell(500L, THIRD_VALUE, 600L);
        sweepTimestamp.set(450L);

        assertThat(singleBucketSweepTask.runOneIteration(SWEEPABLE_CONSERVATIVE_BUCKET_ZERO))
                .as("two entries should have been read from the sweep queue")
                .isEqualTo(2);

        assertThat(keyValueService.get(CONSERVATIVE_TABLE, ImmutableMap.of(DEFAULT_CELL, 499L)))
                .as("sweep should NOT have deleted the second version of the default cell")
                .hasSize(1)
                .hasEntrySatisfying(DEFAULT_CELL, value -> {
                    assertThat(value.getContents()).isEqualTo(ANOTHER_VALUE);
                    assertThat(value.getTimestamp()).isEqualTo(300L);
                });
        assertThat(keyValueService.get(CONSERVATIVE_TABLE, ImmutableMap.of(DEFAULT_CELL, 299L)))
                .as("sweep should have placed a tombstone on the older version of the default cell")
                .hasEntrySatisfying(DEFAULT_CELL, value -> {
                    assertThat(value.getContents()).isEmpty();
                    assertThat(value.getTimestamp()).isEqualTo(-1L);
                });

        assertThat(bucketProgressStore.getBucketProgress(CONSERVATIVE_BUCKET_ZERO))
                .as("bucket progress is updated up to the correct point in the queue")
                .hasValue(BucketProgress.createForTimestampProgress(450L - 1));
        // The bucket has not been completed yet, because it is still not fully swept.
        verify(sweepBucketsTable, never()).deleteBucketEntry(CONSERVATIVE_BUCKET_ZERO);
    }

    // [vw x-----x][          ]
    //        ^ Sweep Timestamp
    //      ^ Sweep Task
    @Test
    public void sweepsFromClosedBucketUpToCellCommittingAfterSweepTimestamp() {
        writeCell(100L, DEFAULT_VALUE, 200L);
        writeCell(300L, ANOTHER_VALUE, 400L);
        writeCell(500L, ANOTHER_VALUE, 600L);
        sweepTimestamp.set(555L);

        assertThat(singleBucketSweepTask.runOneIteration(SWEEPABLE_CONSERVATIVE_BUCKET_ZERO))
                .as("three entries should have been read from the sweep queue")
                .isEqualTo(3);

        assertThat(keyValueService.get(CONSERVATIVE_TABLE, ImmutableMap.of(DEFAULT_CELL, 499L)))
                .as("sweep should NOT have deleted the second version of the default cell")
                .hasSize(1)
                .hasEntrySatisfying(DEFAULT_CELL, value -> {
                    assertThat(value.getContents()).isEqualTo(ANOTHER_VALUE);
                    assertThat(value.getTimestamp()).isEqualTo(300L);
                });
        assertThat(keyValueService.get(CONSERVATIVE_TABLE, ImmutableMap.of(DEFAULT_CELL, 299L)))
                .as("sweep should have placed a tombstone on the older version of the default cell")
                .hasEntrySatisfying(DEFAULT_CELL, value -> {
                    assertThat(value.getContents()).isEmpty();
                    assertThat(value.getTimestamp()).isEqualTo(-1L);
                });

        assertThat(bucketProgressStore.getBucketProgress(CONSERVATIVE_BUCKET_ZERO))
                .as("bucket progress is updated up to the correct point in the queue")
                .hasValue(BucketProgress.createForTimestampProgress(500L - 1));
        // The bucket has not been completed yet, because it is still not fully swept.
        verify(sweepBucketsTable, never()).deleteBucketEntry(CONSERVATIVE_BUCKET_ZERO);
    }

    // [v        w][          ]
    //                  ^ Sweep Timestamp
    //                  ^ Sweep Task
    @Test
    public void doesNotSweepValuesInOtherBuckets() {
        writeTwoCells();
        sweepTimestamp.set(4500L);

        assertThat(singleBucketSweepTask.runOneIteration(SWEEPABLE_CONSERVATIVE_BUCKET_ONE))
                .as("no entries should have been read from the sweep queue, because this task should not be"
                        + " sweeping bucket zero")
                .isEqualTo(0);

        assertThat(keyValueService.get(CONSERVATIVE_TABLE, ImmutableMap.of(DEFAULT_CELL, 250L)))
                .as("sweep should not have run yet on the older version of the default cell")
                .hasEntrySatisfying(DEFAULT_CELL, value -> {
                    assertThat(value.getContents()).isEqualTo(DEFAULT_VALUE);
                    assertThat(value.getTimestamp()).isEqualTo(100L);
                });

        assertThat(bucketProgressStore.getBucketProgress(CONSERVATIVE_BUCKET_ZERO))
                .as("bucket progress for other buckets is untouched")
                .isEmpty();
        assertThat(bucketProgressStore.getBucketProgress(CONSERVATIVE_BUCKET_ONE))
                .as("bucket progress is updated up to the sweep timestamp (non-inclusively)")
                .hasValue(BucketProgress.createForTimestampProgress(4500
                        - SWEEPABLE_CONSERVATIVE_BUCKET_ONE.timestampRange().startInclusive()
                        - 1));
        // The bucket has not been completed yet, because it is still not fully swept.
        verify(sweepBucketsTable, never()).deleteBucketEntry(any());
    }

    // [v        w][xy        ]
    //                  ^ Sweep Timestamp
    //       ^ Sweep Task
    @Test
    public void sweepsBucketAndDeletesBucketGuaranteedNotToBeRecreatedByAssigner() {
        when(sweepBucketAssignerStateMachineTable.getBucketStateAndIdentifier())
                .thenReturn(BucketStateAndIdentifier.of(
                        1, BucketAssignerState.start(1000))); // All buckets at 0 are guaranteed closed
        writeTwoCells();
        writeCell(1111L, PtBytes.toBytes("quux"), 1112L);
        writeCell(1113L, PtBytes.toBytes("qaimaqam"), 1114L);
        sweepTimestamp.set(1337L);

        assertThat(singleBucketSweepTask.runOneIteration(SWEEPABLE_CONSERVATIVE_BUCKET_ZERO))
                .as("two entries should have been read from the sweep queue")
                .isEqualTo(2);

        assertThat(keyValueService.get(CONSERVATIVE_TABLE, ImmutableMap.of(DEFAULT_CELL, 1112L)))
                .as("in bucket 1, sweep should not have run yet on the older version of the default cell")
                .hasEntrySatisfying(DEFAULT_CELL, value -> {
                    assertThat(value.getContents()).isEqualTo(PtBytes.toBytes("quux"));
                    assertThat(value.getTimestamp()).isEqualTo(1111L);
                });

        assertThat(bucketProgressStore.getBucketProgress(CONSERVATIVE_BUCKET_ZERO))
                .as("bucket 0 is completely swept")
                .hasValue(BucketProgress.createForTimestampProgress(SWEEPABLE_CONSERVATIVE_BUCKET_ZERO
                                .timestampRange()
                                .endExclusive()
                        - SWEEPABLE_CONSERVATIVE_BUCKET_ZERO.timestampRange().startInclusive()
                        - 1));
        verify(sweepBucketsTable, times(1)).deleteBucketEntry(CONSERVATIVE_BUCKET_ZERO);
    }

    // [v        w)
    //                  ^ Sweep Timestamp
    //       ^ Sweep Task
    @Test
    public void sweepsButWillNotDeleteBucketThatCouldBeRecreatedByAssigner() {
        when(sweepBucketAssignerStateMachineTable.getBucketStateAndIdentifier())
                .thenReturn(BucketStateAndIdentifier.of(
                        0,
                        BucketAssignerState.immediatelyClosing(
                                0, END_OF_BUCKET_ZERO))); // This can happen if we have more shards
        writeTwoCells();
        sweepTimestamp.set(1337L);

        assertThat(singleBucketSweepTask.runOneIteration(SWEEPABLE_CONSERVATIVE_BUCKET_ZERO))
                .as("two entries should have been read from the sweep queue")
                .isEqualTo(2);

        assertThat(bucketProgressStore.getBucketProgress(CONSERVATIVE_BUCKET_ZERO))
                .as("bucket 0 is completely swept")
                .hasValue(BucketProgress.createForTimestampProgress(SWEEPABLE_CONSERVATIVE_BUCKET_ZERO
                                .timestampRange()
                                .endExclusive()
                        - SWEEPABLE_CONSERVATIVE_BUCKET_ZERO.timestampRange().startInclusive()
                        - 1));

        // We don't delete the bucket in this case BECAUSE it might get rewritten by the state machine
        verify(sweepBucketsTable, never()).deleteBucketEntry(CONSERVATIVE_BUCKET_ZERO);
    }

    // [          ][          ][vwx       )
    //                               ^ Sweep Timestamp
    //                               ^ Sweep Task
    @Test
    public void makesProgressUpToSweepTimestampOnAnOpenBucketButWillNotDeleteIt() {
        when(sweepBucketAssignerStateMachineTable.getBucketStateAndIdentifier())
                .thenReturn(BucketStateAndIdentifier.of(2, BucketAssignerState.waitingUntilCloseable(5000L)));
        writeCell(5200L, DEFAULT_VALUE, 5900L);
        writeCell(6400L, ANOTHER_VALUE, 7300L);
        writeTransactionalDelete(7500L, 7800L);
        sweepTimestamp.set(8000L);

        assertThat(singleBucketSweepTask.runOneIteration(SWEEPABLE_CONSERVATIVE_BUCKET_TWO))
                .as("three entries should have been read from the sweep queue")
                .isEqualTo(3);

        assertThat(bucketProgressStore.getBucketProgress(CONSERVATIVE_BUCKET_TWO))
                .as("bucket 2 makes progress up to the sweep timestamp")
                .hasValue(BucketProgress.createForTimestampProgress(8000L
                        - SWEEPABLE_CONSERVATIVE_BUCKET_TWO.timestampRange().startInclusive()
                        - 1));

        // We don't delete the bucket in this case because it is still open
        verify(sweepBucketsTable, never()).deleteBucketEntry(CONSERVATIVE_BUCKET_TWO);
    }

    // [          ][          ][vwxyz01234)
    //                               ^ Sweep Timestamp
    //                               ^ Sweep Task
    @Test
    public void makesPartialProgressOnAnOpenBucket() {
        when(sweepBucketAssignerStateMachineTable.getBucketStateAndIdentifier())
                .thenReturn(BucketStateAndIdentifier.of(2, BucketAssignerState.waitingUntilCloseable(5000L)));
        int numberOfCellsSweptInOneIteration = SweepQueueUtils.SWEEP_BATCH_SIZE;
        int numberOfCellsWritten = 2 * numberOfCellsSweptInOneIteration + 1;
        for (int i = 0; i < numberOfCellsWritten; i++) {
            int transactionTimestamp = 5000 + i;
            writeCell(transactionTimestamp, PtBytes.toBytes(i), transactionTimestamp);
        }
        int partialProgressOnSecondBatch = 5000;
        sweepTimestamp.set(5000 + numberOfCellsSweptInOneIteration + partialProgressOnSecondBatch);

        assertThat(singleBucketSweepTask.runOneIteration(SWEEPABLE_CONSERVATIVE_BUCKET_TWO))
                .isEqualTo(numberOfCellsSweptInOneIteration);
        assertThat(bucketProgressStore.getBucketProgress(CONSERVATIVE_BUCKET_TWO))
                .as("bucket 2 has partial progress up to the first batch read")
                .hasValue(BucketProgress.createForTimestampProgress(SweepQueueUtils.SWEEP_BATCH_SIZE - 1));

        assertThat(singleBucketSweepTask.runOneIteration(SWEEPABLE_CONSERVATIVE_BUCKET_TWO))
                .isEqualTo(5000);
        assertThat(bucketProgressStore.getBucketProgress(CONSERVATIVE_BUCKET_TWO))
                .as("bucket 2 has partial progress up to the second batch read")
                .hasValue(BucketProgress.createForTimestampProgress(SweepQueueUtils.SWEEP_BATCH_SIZE + 5000 - 1));

        assertThat(singleBucketSweepTask.runOneIteration(SWEEPABLE_CONSERVATIVE_BUCKET_TWO))
                .as("no further progress is made past the sweep timestamp")
                .isEqualTo(0);

        verify(sweepBucketsTable, never()).deleteBucketEntry(CONSERVATIVE_BUCKET_TWO);
    }

    private void writeTwoCells() {
        writeCell(100L, DEFAULT_VALUE, 200L);
        writeCell(300L, ANOTHER_VALUE, 400L);
    }

    private void writeCell(long startTimestamp, byte[] defaultValue, long commitTimestamp) {
        sweepQueueWriter.enqueue(ImmutableList.of(WriteInfo.write(CONSERVATIVE_TABLE, DEFAULT_CELL, startTimestamp)));
        keyValueService.put(CONSERVATIVE_TABLE, ImmutableMap.of(DEFAULT_CELL, defaultValue), startTimestamp);
        transactionService.putUnlessExists(startTimestamp, commitTimestamp);
    }

    private void writeTransactionalDelete(long startTimestamp, long commitTimestamp) {
        sweepQueueWriter.enqueue(
                ImmutableList.of(WriteInfo.tombstone(CONSERVATIVE_TABLE, DEFAULT_CELL, startTimestamp)));
        keyValueService.put(
                CONSERVATIVE_TABLE, ImmutableMap.of(DEFAULT_CELL, PtBytes.EMPTY_BYTE_ARRAY), startTimestamp);
        transactionService.putUnlessExists(startTimestamp, commitTimestamp);
    }

    private static final class TestBucketProgressStore implements BucketProgressStore {
        private final Map<Bucket, BucketProgress> bucketToProgress = new ConcurrentHashMap<>();

        @Override
        public Optional<BucketProgress> getBucketProgress(Bucket bucket) {
            return Optional.ofNullable(bucketToProgress.get(bucket));
        }

        @Override
        public void updateBucketProgressToAtLeast(Bucket bucket, BucketProgress minimum) {
            bucketToProgress.merge(bucket, minimum, (a, b) -> a.compareTo(b) > 0 ? a : b);
        }

        @Override
        public void deleteBucketProgress(Bucket bucket) {
            bucketToProgress.remove(bucket);
        }
    }
}
