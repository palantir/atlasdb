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
import com.palantir.atlasdb.sweep.asts.bucketingthings.BucketCompletionListener;
import com.palantir.atlasdb.sweep.asts.bucketingthings.CompletelyClosedSweepBucketBoundRetriever;
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
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.service.SimpleTransactionService;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.time.Clock;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import one.util.streamex.EntryStream;
import org.immutables.value.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class SingleBucketSweepTaskIntegrationTest {
    private static final int SHARDS = 1; // Used to avoid complications of the hash function

    private static final TableReference CONSERVATIVE_TABLE = TableReference.createFromFullyQualifiedName("terri.tory");
    private static final TableReference THOROUGH_TABLE = TableReference.createFromFullyQualifiedName("comp.lete");

    private static final long END_OF_BUCKET_ZERO = 1000L;
    private static final long END_OF_BUCKET_ONE = 5000L;
    private static final TimestampRange BUCKET_ZERO_TIMESTAMP_RANGE = TimestampRange.of(0L, END_OF_BUCKET_ZERO);
    private static final TimestampRange BUCKET_ONE_TIMESTAMP_RANGE =
            TimestampRange.of(END_OF_BUCKET_ZERO, END_OF_BUCKET_ONE);
    private static final TimestampRange BUCKET_TWO_TIMESTAMP_RANGE =
            TimestampRange.of(END_OF_BUCKET_ONE, -1L); // open bucket

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
            _unused -> Optional.empty()); // Telemetry, not used for this test
    private final SweepQueueWriter sweepQueueWriter =
            new SweepQueueWriter(sweepableTimestamps, sweepableCells, writeInfoPartitioner);

    private final BucketProgressStore bucketProgressStore = new TestBucketProgressStore();

    private BucketCompletionListener bucketCompletionListener;
    private CompletelyClosedSweepBucketBoundRetriever completelyClosedSweepBucketBoundRetriever;

    private SingleBucketSweepTask singleBucketSweepTask;

    @BeforeEach
    public void setUp() {
        bucketCompletionListener = mock(BucketCompletionListener.class);
        completelyClosedSweepBucketBoundRetriever = mock(CompletelyClosedSweepBucketBoundRetriever.class);
        singleBucketSweepTask = new DefaultSingleBucketSweepTask(
                bucketProgressStore,
                sweepQueueReader,
                sweepQueueDeleter,
                sweepTimestamp::get,
                targetedSweepMetrics,
                bucketCompletionListener,
                completelyClosedSweepBucketBoundRetriever);

        keyValueService.createTable(
                CONSERVATIVE_TABLE,
                TableMetadata.builder()
                        .sweepStrategy(SweepStrategy.CONSERVATIVE)
                        .build()
                        .persistToBytes());
        keyValueService.createTable(
                THOROUGH_TABLE,
                TableMetadata.builder()
                        .sweepStrategy(SweepStrategy.THOROUGH)
                        .build()
                        .persistToBytes());
    }

    // [vw       x]
    //      ^ Sweep Timestamp
    //      ^ Sweep Task
    @ParameterizedTest
    @MethodSource("testContexts")
    public void sweepsFromClosedBucketUpToSweepTimestamp(SweepStrategyTestContext sweepStrategyTestContext) {
        writeCell(sweepStrategyTestContext.dataTable(), 100L, DEFAULT_VALUE, 200L);
        writeCell(sweepStrategyTestContext.dataTable(), 300L, ANOTHER_VALUE, 400L);
        writeCell(sweepStrategyTestContext.dataTable(), 500L, THIRD_VALUE, 600L);
        sweepTimestamp.set(450L);

        assertThat(singleBucketSweepTask.runOneIteration(SweepableBucket.of(
                        sweepStrategyTestContext.bucketFactory().apply(0), BUCKET_ZERO_TIMESTAMP_RANGE)))
                .as("two entries should have been read from the sweep queue")
                .isEqualTo(2);

        assertThat(keyValueService.get(sweepStrategyTestContext.dataTable(), ImmutableMap.of(DEFAULT_CELL, 499L)))
                .as("sweep should NOT have deleted the second version of the default cell")
                .hasSize(1)
                .hasEntrySatisfying(DEFAULT_CELL, value -> {
                    assertThat(value.getContents()).isEqualTo(ANOTHER_VALUE);
                    assertThat(value.getTimestamp()).isEqualTo(300L);
                });
        checkValueSwept(DEFAULT_CELL, 299L, sweepStrategyTestContext);

        assertThat(bucketProgressStore.getBucketProgress(
                        sweepStrategyTestContext.bucketFactory().apply(0)))
                .as("bucket progress is updated to the sweep timestamp (non-inclusively)")
                .hasValue(BucketProgress.createForTimestampProgress(450L - 1));
        // The bucket has not been completed yet, because it is still not fully swept.
        verify(bucketCompletionListener, never())
                .markBucketCompleteAndRemoveFromScheduling(
                        sweepStrategyTestContext.bucketFactory().apply(0));
    }

    // [vw x-----x]
    //        ^ Sweep Timestamp
    //      ^ Sweep Task
    @ParameterizedTest
    @MethodSource("testContexts")
    public void sweepsFromClosedBucketUpToCellCommittingAfterSweepTimestamp(
            SweepStrategyTestContext sweepStrategyTestContext) {
        writeCell(sweepStrategyTestContext.dataTable(), 100L, DEFAULT_VALUE, 200L);
        writeCell(sweepStrategyTestContext.dataTable(), 300L, ANOTHER_VALUE, 400L);
        writeCell(sweepStrategyTestContext.dataTable(), 500L, THIRD_VALUE, 600L);
        sweepTimestamp.set(555L);

        assertThat(singleBucketSweepTask.runOneIteration(SweepableBucket.of(
                        sweepStrategyTestContext.bucketFactory().apply(0), BUCKET_ZERO_TIMESTAMP_RANGE)))
                .as("three entries should have been read from the sweep queue")
                .isEqualTo(3);

        assertThat(keyValueService.get(sweepStrategyTestContext.dataTable(), ImmutableMap.of(DEFAULT_CELL, 499L)))
                .as("sweep should NOT have deleted the second version of the default cell")
                .hasSize(1)
                .hasEntrySatisfying(DEFAULT_CELL, value -> {
                    assertThat(value.getContents()).isEqualTo(ANOTHER_VALUE);
                    assertThat(value.getTimestamp()).isEqualTo(300L);
                });
        checkValueSwept(DEFAULT_CELL, 299L, sweepStrategyTestContext);

        assertThat(bucketProgressStore.getBucketProgress(
                        sweepStrategyTestContext.bucketFactory().apply(0)))
                .as("bucket progress is updated up to the correct point in the queue")
                .hasValue(BucketProgress.createForTimestampProgress(500L - 1));
        // The bucket has not been completed yet, because it is still not fully swept.
        verify(bucketCompletionListener, never())
                .markBucketCompleteAndRemoveFromScheduling(
                        sweepStrategyTestContext.bucketFactory().apply(0));
    }

    // [v        w][          ]
    //                  ^ Sweep Timestamp
    //                  ^ Sweep Task
    @ParameterizedTest
    @MethodSource("testContexts")
    public void doesNotSweepValuesInOtherBuckets(SweepStrategyTestContext sweepStrategyTestContext) {
        writeTwoCells(sweepStrategyTestContext.dataTable());
        sweepTimestamp.set(4500L);

        assertThat(singleBucketSweepTask.runOneIteration(SweepableBucket.of(
                        sweepStrategyTestContext.bucketFactory().apply(1), BUCKET_ONE_TIMESTAMP_RANGE)))
                .as("no entries should have been read from the sweep queue, because this task should not be"
                        + " sweeping bucket zero")
                .isEqualTo(0);

        assertThat(keyValueService.get(sweepStrategyTestContext.dataTable(), ImmutableMap.of(DEFAULT_CELL, 250L)))
                .as("sweep should not have run yet on the older version of the default cell")
                .hasEntrySatisfying(DEFAULT_CELL, value -> {
                    assertThat(value.getContents()).isEqualTo(DEFAULT_VALUE);
                    assertThat(value.getTimestamp()).isEqualTo(100L);
                });

        assertThat(bucketProgressStore.getBucketProgress(
                        sweepStrategyTestContext.bucketFactory().apply(0)))
                .as("bucket progress for other buckets is untouched")
                .isEmpty();
        assertThat(bucketProgressStore.getBucketProgress(
                        sweepStrategyTestContext.bucketFactory().apply(1)))
                .as("bucket progress is updated up to the sweep timestamp (non-inclusively)")
                .hasValue(BucketProgress.createForTimestampProgress(4500 - END_OF_BUCKET_ZERO - 1));
        // The bucket has not been completed yet, because it is still not fully swept.
        verify(bucketCompletionListener, never()).markBucketCompleteAndRemoveFromScheduling(any());
    }

    // [v        w]
    //                  ^ Sweep Timestamp
    //       ^ Sweep Task
    @ParameterizedTest
    @MethodSource("testContexts")
    public void sweepsBucketAndDeletesBucketGuaranteedNotToBeRecreatedByAssigner(
            SweepStrategyTestContext sweepStrategyTestContext) {
        when(completelyClosedSweepBucketBoundRetriever.getStrictUpperBoundForCompletelyClosedBuckets())
                .thenReturn(1L); // All buckets at 0 are guaranteed closed
        writeTwoCells(sweepStrategyTestContext.dataTable());
        sweepTimestamp.set(1337L);

        assertThat(singleBucketSweepTask.runOneIteration(SweepableBucket.of(
                        sweepStrategyTestContext.bucketFactory().apply(0), BUCKET_ZERO_TIMESTAMP_RANGE)))
                .as("two entries should have been read from the sweep queue")
                .isEqualTo(2);

        assertThat(bucketProgressStore.getBucketProgress(
                        sweepStrategyTestContext.bucketFactory().apply(0)))
                .as("bucket 0 is completely swept")
                .hasValue(BucketProgress.createForTimestampProgress(END_OF_BUCKET_ZERO - 1));
        verify(bucketCompletionListener, times(1))
                .markBucketCompleteAndRemoveFromScheduling(
                        sweepStrategyTestContext.bucketFactory().apply(0));
    }

    // [v        w)
    //                  ^ Sweep Timestamp
    //       ^ Sweep Task
    @ParameterizedTest
    @MethodSource("testContexts")
    public void sweepsButWillNotDeleteBucketThatCouldBeRecreatedByAssigner(
            SweepStrategyTestContext sweepStrategyTestContext) {
        when(completelyClosedSweepBucketBoundRetriever.getStrictUpperBoundForCompletelyClosedBuckets())
                .thenReturn(0L); // This can happen if we have more shards
        writeTwoCells(sweepStrategyTestContext.dataTable());
        sweepTimestamp.set(1337L);

        assertThat(singleBucketSweepTask.runOneIteration(SweepableBucket.of(
                        sweepStrategyTestContext.bucketFactory().apply(0), BUCKET_ZERO_TIMESTAMP_RANGE)))
                .as("two entries should have been read from the sweep queue")
                .isEqualTo(2);

        assertThat(bucketProgressStore.getBucketProgress(
                        sweepStrategyTestContext.bucketFactory().apply(0)))
                .as("bucket 0 is completely swept")
                .hasValue(BucketProgress.createForTimestampProgress(END_OF_BUCKET_ZERO - 1));

        // We don't delete the bucket in this case BECAUSE it might get rewritten by the state machine
        verify(bucketCompletionListener, never())
                .markBucketCompleteAndRemoveFromScheduling(
                        sweepStrategyTestContext.bucketFactory().apply(0));
    }

    // [v        w)
    //       ^ Progress
    //                  ^ Sweep Timestamp
    //       ^ Sweep Task
    @ParameterizedTest
    @MethodSource("testContexts")
    public void resumesProgressFromStoredProgress(SweepStrategyTestContext sweepStrategyTestContext) {
        when(completelyClosedSweepBucketBoundRetriever.getStrictUpperBoundForCompletelyClosedBuckets())
                .thenReturn(2L);
        writeTwoCells(sweepStrategyTestContext.dataTable());
        sweepTimestamp.set(END_OF_BUCKET_ZERO);
        bucketProgressStore.updateBucketProgressToAtLeast(
                sweepStrategyTestContext.bucketFactory().apply(0), BucketProgress.createForTimestampProgress(250L));
        assertThat(singleBucketSweepTask.runOneIteration(SweepableBucket.of(
                        sweepStrategyTestContext.bucketFactory().apply(0), BUCKET_ZERO_TIMESTAMP_RANGE)))
                .as("only one entry should be read from the sweep queue, because the first write info is behind"
                        + " the progress pointer")
                .isEqualTo(1);
    }

    // [v        w)
    //            ^ Progress
    //                  ^ Sweep Timestamp
    //       ^ Sweep Task
    @ParameterizedTest
    @MethodSource("testContexts")
    public void doesNotRereadEntriesAndReattemptsDeleteOnCompletedBucket_NotDeleteable(
            SweepStrategyTestContext sweepStrategyTestContext) {
        when(completelyClosedSweepBucketBoundRetriever.getStrictUpperBoundForCompletelyClosedBuckets())
                .thenReturn(0L); // This can happen if we have more shards
        writeTwoCells(sweepStrategyTestContext.dataTable());
        sweepTimestamp.set(1337L);

        bucketProgressStore.updateBucketProgressToAtLeast(
                sweepStrategyTestContext.bucketFactory().apply(0),
                BucketProgress.createForTimestampProgress(END_OF_BUCKET_ZERO - 1));
        assertThat(singleBucketSweepTask.runOneIteration(SweepableBucket.of(
                        sweepStrategyTestContext.bucketFactory().apply(0), BUCKET_ZERO_TIMESTAMP_RANGE)))
                .as("no entries should have been read from the sweep queue, because the bucket had already been"
                        + " completed")
                .isEqualTo(0);
        verify(bucketCompletionListener, never())
                .markBucketCompleteAndRemoveFromScheduling(
                        sweepStrategyTestContext.bucketFactory().apply(0));
    }

    // [v        w)
    //            ^ Progress
    //                  ^ Sweep Timestamp
    //       ^ Sweep Task
    @ParameterizedTest
    @MethodSource("testContexts")
    public void doesNotRereadEntriesAndReattemptsDeleteOnCompletedBucket_Deleteable(
            SweepStrategyTestContext sweepStrategyTestContext) {
        when(completelyClosedSweepBucketBoundRetriever.getStrictUpperBoundForCompletelyClosedBuckets())
                .thenReturn(1L); // This can happen if we have more shards
        writeTwoCells(sweepStrategyTestContext.dataTable());
        sweepTimestamp.set(1337L);

        bucketProgressStore.updateBucketProgressToAtLeast(
                sweepStrategyTestContext.bucketFactory().apply(0),
                BucketProgress.createForTimestampProgress(END_OF_BUCKET_ZERO - 1));
        assertThat(singleBucketSweepTask.runOneIteration(SweepableBucket.of(
                        sweepStrategyTestContext.bucketFactory().apply(0), BUCKET_ZERO_TIMESTAMP_RANGE)))
                .as("no entries should have been read from the sweep queue, because the bucket had already been"
                        + " completed")
                .isEqualTo(0);
        verify(bucketCompletionListener, times(1))
                .markBucketCompleteAndRemoveFromScheduling(
                        sweepStrategyTestContext.bucketFactory().apply(0));
    }

    // [          ][          ][vw        )
    //               ^ Sweep Timestamp
    //                               ^ Sweep Task
    @ParameterizedTest
    @MethodSource("testContexts")
    public void doesNotReadEntriesFromQueueIfSweepTimestampTooEarlyForBucket(
            SweepStrategyTestContext sweepStrategyTestContext) {
        writeCell(sweepStrategyTestContext.dataTable(), 5200L, DEFAULT_VALUE, 5900L);
        writeCell(sweepStrategyTestContext.dataTable(), 6400L, ANOTHER_VALUE, 7300L);
        sweepTimestamp.set(END_OF_BUCKET_ZERO + 1);
        assertThat(singleBucketSweepTask.runOneIteration(SweepableBucket.of(
                        sweepStrategyTestContext.bucketFactory().apply(2), BUCKET_TWO_TIMESTAMP_RANGE)))
                .as("nothing should have been read from the queue, because sweep can't enter bucket 2")
                .isEqualTo(0);
    }

    // [          ][          ][vwx       )
    //                               ^ Sweep Timestamp
    //                               ^ Sweep Task
    @ParameterizedTest
    @MethodSource("testContexts")
    public void makesProgressUpToSweepTimestampOnAnOpenBucketButWillNotDeleteIt(
            SweepStrategyTestContext sweepStrategyTestContext) {
        when(completelyClosedSweepBucketBoundRetriever.getStrictUpperBoundForCompletelyClosedBuckets())
                .thenReturn(2L);
        writeCell(sweepStrategyTestContext.dataTable(), 5200L, DEFAULT_VALUE, 5900L);
        writeCell(sweepStrategyTestContext.dataTable(), 6400L, ANOTHER_VALUE, 7300L);
        writeTransactionalDelete(sweepStrategyTestContext.dataTable(), 7500L, 7800L);
        sweepTimestamp.set(8000L);

        assertThat(singleBucketSweepTask.runOneIteration(SweepableBucket.of(
                        sweepStrategyTestContext.bucketFactory().apply(2), BUCKET_TWO_TIMESTAMP_RANGE)))
                .as("three entries should have been read from the sweep queue")
                .isEqualTo(3);

        assertThat(bucketProgressStore.getBucketProgress(
                        sweepStrategyTestContext.bucketFactory().apply(2)))
                .as("bucket 2 makes progress up to the sweep timestamp")
                .hasValue(BucketProgress.createForTimestampProgress(8000L - END_OF_BUCKET_ONE - 1));

        // We don't delete the bucket in this case because it is still open
        verify(bucketCompletionListener, never())
                .markBucketCompleteAndRemoveFromScheduling(
                        sweepStrategyTestContext.bucketFactory().apply(2));
    }

    // [          ][          ][vwxyz01234)
    //                               ^ Sweep Timestamp
    //                               ^ Sweep Task
    @ParameterizedTest
    @MethodSource("testContexts")
    public void makesPartialProgressOnAnOpenBucket(SweepStrategyTestContext sweepStrategyTestContext) {
        when(completelyClosedSweepBucketBoundRetriever.getStrictUpperBoundForCompletelyClosedBuckets())
                .thenReturn(2L);
        int numberOfCellsSweptInOneIteration = SweepQueueUtils.SWEEP_BATCH_SIZE;
        int numberOfCellsWritten = 2 * numberOfCellsSweptInOneIteration + 1;
        List<KnownTableWrite> knownTableWrites = LongStream.range(0, numberOfCellsWritten)
                .mapToObj(index -> {
                    long transactionTimestamp = END_OF_BUCKET_ONE + index;
                    return KnownTableWrite.builder()
                            .startTimestamp(transactionTimestamp)
                            .commitTimestamp(transactionTimestamp)
                            .cell(DEFAULT_CELL)
                            .value(PtBytes.toBytes(index))
                            .build();
                })
                .collect(Collectors.toList());

        writeCells(sweepStrategyTestContext.dataTable(), knownTableWrites);

        long partialProgressOnSecondBatch = 3333;
        sweepTimestamp.set(END_OF_BUCKET_ONE + numberOfCellsSweptInOneIteration + partialProgressOnSecondBatch);

        SweepableBucket sweepableBucketTwo =
                SweepableBucket.of(sweepStrategyTestContext.bucketFactory().apply(2), BUCKET_TWO_TIMESTAMP_RANGE);
        assertThat(singleBucketSweepTask.runOneIteration(sweepableBucketTwo))
                .isEqualTo(numberOfCellsSweptInOneIteration);
        assertThat(bucketProgressStore.getBucketProgress(
                        sweepStrategyTestContext.bucketFactory().apply(2)))
                .as("bucket 2 has partial progress up to the first batch read")
                .hasValue(BucketProgress.createForTimestampProgress(SweepQueueUtils.SWEEP_BATCH_SIZE - 1));

        assertThat(singleBucketSweepTask.runOneIteration(sweepableBucketTwo)).isEqualTo(partialProgressOnSecondBatch);
        assertThat(bucketProgressStore.getBucketProgress(
                        sweepStrategyTestContext.bucketFactory().apply(2)))
                .as("bucket 2 has partial progress up to the second batch read")
                .hasValue(BucketProgress.createForTimestampProgress(
                        SweepQueueUtils.SWEEP_BATCH_SIZE + partialProgressOnSecondBatch - 1));

        assertThat(singleBucketSweepTask.runOneIteration(sweepableBucketTwo))
                .as("no further progress is made past the sweep timestamp")
                .isEqualTo(0);

        verify(bucketCompletionListener, never())
                .markBucketCompleteAndRemoveFromScheduling(
                        sweepStrategyTestContext.bucketFactory().apply(2));
    }

    // [          ][          ][vwxyz01   )
    //                                 ^ Sweep Timestamp
    //                               ^ Sweep Task
    // except that the values are spaced far apart in timestamps, and so in the underlying sweep queue storage.
    @ParameterizedTest
    @MethodSource("testContexts")
    public void makesProgressOnCellsInDifferentSweepQueuePartitions(SweepStrategyTestContext sweepStrategyTestContext) {
        when(completelyClosedSweepBucketBoundRetriever.getStrictUpperBoundForCompletelyClosedBuckets())
                .thenReturn(2L);
        int numberOfCellsToWrite = 79;
        long timestampIncrement = SweepQueueUtils.TS_COARSE_GRANULARITY + SweepQueueUtils.TS_FINE_GRANULARITY;

        long currentTimestamp = END_OF_BUCKET_ONE;
        List<KnownTableWrite> knownTableWrites = new ArrayList<>();
        for (int i = 0; i < numberOfCellsToWrite; i++) {
            knownTableWrites.add(KnownTableWrite.builder()
                    .startTimestamp(currentTimestamp)
                    .commitTimestamp(currentTimestamp)
                    .cell(DEFAULT_CELL)
                    .value(PtBytes.toBytes(currentTimestamp))
                    .build());
            currentTimestamp += timestampIncrement;
        }
        writeCells(sweepStrategyTestContext.dataTable(), knownTableWrites);
        sweepTimestamp.set(currentTimestamp + 1);

        SweepableBucket sweepableBucketTwo =
                SweepableBucket.of(sweepStrategyTestContext.bucketFactory().apply(2), BUCKET_TWO_TIMESTAMP_RANGE);
        // We don't care exactly how the sweep batching works, other than that we are able to make progress.
        // + 1 because we need to explicitly discover there is nothing after the last cell
        int allowedIterations = numberOfCellsToWrite + 1;
        long entriesRead = 0;
        for (int i = 0; i < allowedIterations; i++) {
            entriesRead += singleBucketSweepTask.runOneIteration(sweepableBucketTwo);
        }
        assertThat(entriesRead)
                .as("all cells that were enqueued should eventually be read")
                .isEqualTo(numberOfCellsToWrite);

        assertThat(bucketProgressStore.getBucketProgress(
                        sweepStrategyTestContext.bucketFactory().apply(2)))
                .hasValueSatisfying(progress -> assertThat(progress.timestampProgress())
                        .as("after the allowed number of iterations, sweep made progress within the same bucket")
                        .isEqualTo(sweepTimestamp.get() - 1 - END_OF_BUCKET_ONE));
    }

    @ParameterizedTest
    @MethodSource("testContexts")
    public void doesNotSweepCellsForDifferentShards(SweepStrategyTestContext sweepStrategyTestContext) {
        when(completelyClosedSweepBucketBoundRetriever.getStrictUpperBoundForCompletelyClosedBuckets())
                .thenReturn(2L);
        writeTwoCells(sweepStrategyTestContext.dataTable());
        sweepTimestamp.set(END_OF_BUCKET_ONE);

        assertThat(singleBucketSweepTask.runOneIteration(SweepableBucket.of(
                        Bucket.of(ShardAndStrategy.of(42, sweepStrategyTestContext.strategy()), 0),
                        BUCKET_ZERO_TIMESTAMP_RANGE)))
                .as("the writes should have been enqueued to shard zero, not 42")
                .isEqualTo(0);
        assertThat(bucketProgressStore.getBucketProgress(
                        sweepStrategyTestContext.bucketFactory().apply(0)))
                .as("we should not have swept bucket 0 for the strategy in our context yet")
                .isEmpty();
    }

    @ParameterizedTest
    @MethodSource("testContexts")
    public void doesNotSweepCellsForDifferentStrategies(SweepStrategyTestContext sweepStrategyTestContext) {
        when(completelyClosedSweepBucketBoundRetriever.getStrictUpperBoundForCompletelyClosedBuckets())
                .thenReturn(2L);
        SweeperStrategy otherStrategy = sweepStrategyTestContext.strategy() == SweeperStrategy.CONSERVATIVE
                ? SweeperStrategy.THOROUGH
                : SweeperStrategy.CONSERVATIVE;
        writeTwoCells(sweepStrategyTestContext.dataTable());
        sweepTimestamp.set(END_OF_BUCKET_ONE);

        assertThat(singleBucketSweepTask.runOneIteration(SweepableBucket.of(
                        Bucket.of(ShardAndStrategy.of(0, otherStrategy), 0), BUCKET_ZERO_TIMESTAMP_RANGE)))
                .as("the writes should have been enqueued to the other sweeper strategy")
                .isEqualTo(0);
        assertThat(bucketProgressStore.getBucketProgress(
                        sweepStrategyTestContext.bucketFactory().apply(0)))
                .as("we should not have swept bucket 0 for the strategy in our context yet")
                .isEmpty();
    }

    private void writeTwoCells(TableReference tableReference) {
        writeCell(tableReference, 100L, DEFAULT_VALUE, 200L);
        writeCell(tableReference, 300L, ANOTHER_VALUE, 400L);
    }

    private void writeCell(TableReference tableReference, long startTimestamp, byte[] value, long commitTimestamp) {
        writeCells(
                tableReference,
                ImmutableList.of(KnownTableWrite.builder()
                        .cell(DEFAULT_CELL)
                        .value(value)
                        .startTimestamp(startTimestamp)
                        .commitTimestamp(commitTimestamp)
                        .build()));
    }

    private void writeTransactionalDelete(TableReference tableReference, long startTimestamp, long commitTimestamp) {
        writeCell(tableReference, startTimestamp, PtBytes.EMPTY_BYTE_ARRAY, commitTimestamp);
    }

    private void writeCells(TableReference tableReference, List<KnownTableWrite> knownTableWrites) {
        sweepQueueWriter.enqueue(knownTableWrites.stream()
                .map(knownTableWrite -> getWriteInfo(tableReference, knownTableWrite))
                .collect(Collectors.toList()));

        // Cleverer implementations could try a groupingBy on the start timestamp here, but in the interest of
        // readability and in existing usage we only use one cell at a time, therefore I won't optimise this yet.
        knownTableWrites.forEach(knownTableWrite -> keyValueService.put(
                tableReference,
                ImmutableMap.of(knownTableWrite.cell(), knownTableWrite.value()),
                knownTableWrite.startTimestamp()));

        // If multiple writes say different things about a given start timestamp, this will throw, and users will deal
        // with the fallout.
        Map<Long, Long> startToCommitTimestamps = EntryStream.of(knownTableWrites)
                .mapToKey((_unusedIndex, tableWrite) -> tableWrite.startTimestamp())
                .mapValues(KnownTableWrite::commitTimestamp)
                .toMap();
        transactionService.putUnlessExists(startToCommitTimestamps);
    }

    private void checkValueSwept(Cell cell, long timestamp, SweepStrategyTestContext sweepStrategyTestContext) {
        switch (sweepStrategyTestContext.strategy()) {
            case THOROUGH:
                checkNoValueExistsBefore(sweepStrategyTestContext.dataTable(), cell, timestamp);
                break;
            case CONSERVATIVE:
                checkSentinelExistsBefore(sweepStrategyTestContext.dataTable(), cell, timestamp);
                break;
            default:
                throw new SafeIllegalStateException(
                        "Not expecting to see the included sweep strategy in this test",
                        SafeArg.of("sweepStrategy", sweepStrategyTestContext.strategy()));
        }
    }

    private void checkNoValueExistsBefore(TableReference tableReference, Cell cell, long timestamp) {
        assertThat(keyValueService.get(tableReference, ImmutableMap.of(cell, timestamp)))
                .isEmpty();
    }

    private void checkSentinelExistsBefore(TableReference tableReference, Cell cell, long timestamp) {
        assertThat(keyValueService.get(tableReference, ImmutableMap.of(cell, timestamp)))
                .hasSize(1)
                .hasEntrySatisfying(cell, value -> {
                    assertThat(value.getTimestamp())
                            .isEqualTo(com.palantir.atlasdb.keyvalue.api.Value.INVALID_VALUE_TIMESTAMP);
                    assertThat(com.palantir.atlasdb.keyvalue.api.Value.isTombstone(value.getContents()))
                            .isTrue();
                });
    }

    private static WriteInfo getWriteInfo(TableReference tableReference, KnownTableWrite knownTableWrite) {
        if (Arrays.equals(knownTableWrite.value(), PtBytes.EMPTY_BYTE_ARRAY)) {
            return WriteInfo.tombstone(tableReference, knownTableWrite.cell(), knownTableWrite.startTimestamp());
        } else {
            return WriteInfo.write(tableReference, knownTableWrite.cell(), knownTableWrite.startTimestamp());
        }
    }

    public static Stream<SweepStrategyTestContext> testContexts() {
        return Stream.of(
                SweepStrategyTestContext.builder()
                        .strategy(SweeperStrategy.CONSERVATIVE)
                        .dataTable(CONSERVATIVE_TABLE)
                        .bucketFactory(index -> Bucket.of(ShardAndStrategy.conservative(0), index))
                        .build(),
                SweepStrategyTestContext.builder()
                        .strategy(SweeperStrategy.THOROUGH)
                        .dataTable(THOROUGH_TABLE)
                        .bucketFactory(index -> Bucket.of(ShardAndStrategy.thorough(0), index))
                        .build());
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

    @Value.Immutable
    public interface KnownTableWrite {
        Cell cell();

        byte[] value();

        long startTimestamp();

        long commitTimestamp();

        static ImmutableKnownTableWrite.Builder builder() {
            return ImmutableKnownTableWrite.builder();
        }
    }

    @Value.Immutable
    public interface SweepStrategyTestContext {
        SweeperStrategy strategy();

        TableReference dataTable();

        Function<Integer, Bucket> bucketFactory();

        static ImmutableSweepStrategyTestContext.Builder builder() {
            return ImmutableSweepStrategyTestContext.builder();
        }
    }
}
