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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.Sweeper;
import com.palantir.atlasdb.sweep.asts.SweepableBucket.TimestampRange;
import com.palantir.atlasdb.sweep.asts.bucketingthings.BucketCompletionListener;
import com.palantir.atlasdb.sweep.asts.bucketingthings.CompletelyClosedSweepBucketBoundRetriever;
import com.palantir.atlasdb.sweep.asts.progress.BucketProgress;
import com.palantir.atlasdb.sweep.asts.progress.BucketProgressStore;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.atlasdb.sweep.queue.DedicatedRows;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.SweepBatch;
import com.palantir.atlasdb.sweep.queue.SweepBatchWithPartitionInfo;
import com.palantir.atlasdb.sweep.queue.SweepQueueCleaner;
import com.palantir.atlasdb.sweep.queue.SweepQueueDeleter;
import com.palantir.atlasdb.sweep.queue.SweepQueueReader;
import com.palantir.atlasdb.sweep.queue.SweepQueueUtils;
import com.palantir.atlasdb.sweep.queue.WriteInfo;
import com.palantir.logsafe.Preconditions;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.Stream;
import org.immutables.value.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DefaultSingleBucketSweepTaskTest {
    private static final long MIN_BUCKET_SIZE = 100L;

    public static final TableReference TABLE_REFERENCE = TableReference.createFromFullyQualifiedName("t.table");

    @Mock
    private BucketProgressStore bucketProgressStore;

    @Mock
    private SweepQueueReader sweepQueueReader;

    @Mock
    private SweepQueueDeleter sweepQueueDeleter;

    @Mock
    private SweepQueueCleaner sweepQueueCleaner;

    @Mock
    private LongSupplier sweepTimestampSupplier;

    @Mock
    private TargetedSweepMetrics targetedSweepMetrics;

    @Mock
    private BucketCompletionListener completionListener;

    @Mock
    private CompletelyClosedSweepBucketBoundRetriever boundRetriever;

    private DefaultSingleBucketSweepTask defaultSingleBucketSweepTask;

    @BeforeEach
    public void setUp() {
        defaultSingleBucketSweepTask = new DefaultSingleBucketSweepTask(
                bucketProgressStore,
                sweepQueueReader,
                sweepQueueDeleter,
                sweepQueueCleaner,
                sweepTimestampSupplier,
                targetedSweepMetrics,
                completionListener,
                boundRetriever);
    }

    @ParameterizedTest
    @MethodSource("allSweepBuckets")
    public void returnsEarlyIfSweepTimestampHasNotEnteredTheRelevantBucket(SweepBucketTestContext context) {
        when(sweepTimestampSupplier.getAsLong()).thenReturn(context.startTimestampInclusive() - 1L);
        assertThat(defaultSingleBucketSweepTask.runOneIteration(context.sweepableBucket()))
                .isEqualTo(0);

        verifyNoInteractions(bucketProgressStore, sweepQueueReader, sweepQueueDeleter, sweepQueueCleaner, completionListener);
    }

    @ParameterizedTest
    @MethodSource("closedSweepBuckets")
    public void returnsEarlyAndDeletesDeletableBucketIfEarlierIterationsHaveFullySweptTheBucket(
            SweepBucketTestContext context) {
        when(sweepTimestampSupplier.getAsLong()).thenReturn(Long.MAX_VALUE);
        when(bucketProgressStore.getBucketProgress(context.bucket())).thenReturn(context.completeProgressForBucket());
        when(boundRetriever.getStrictUpperBoundForCompletelyClosedBuckets()).thenReturn(Long.MAX_VALUE);

        assertThat(defaultSingleBucketSweepTask.runOneIteration(context.sweepableBucket()))
                .isEqualTo(0);
        verify(completionListener).markBucketCompleteAndRemoveFromScheduling(context.bucket());
        verifyNoInteractions(sweepQueueReader, sweepQueueDeleter, sweepQueueCleaner);
    }

    @ParameterizedTest
    @MethodSource("closedSweepBuckets")
    public void returnsEarlyButDoesNotDeleteNonDeletableBucketIfEarlierIterationsHaveFullySweptTheBucket(
            SweepBucketTestContext context) {
        when(sweepTimestampSupplier.getAsLong()).thenReturn(Long.MAX_VALUE);
        when(bucketProgressStore.getBucketProgress(context.bucket())).thenReturn(context.completeProgressForBucket());
        when(boundRetriever.getStrictUpperBoundForCompletelyClosedBuckets()).thenReturn(context.bucketIdentifier());

        assertThat(defaultSingleBucketSweepTask.runOneIteration(context.sweepableBucket()))
                .isEqualTo(0);
        verifyNoInteractions(completionListener, sweepQueueReader, sweepQueueDeleter, sweepQueueCleaner);
    }

    @ParameterizedTest
    @MethodSource("allSweepBuckets")
    public void returnsEarlyIfSweepTimestampIsBehindPastBucketProgress(SweepBucketTestContext context) {
        when(sweepTimestampSupplier.getAsLong()).thenReturn(context.startTimestampInclusive() + 4L);
        when(bucketProgressStore.getBucketProgress(context.bucket()))
                .thenReturn(Optional.of(BucketProgress.createForTimestampProgress(5L)));

        assertThat(defaultSingleBucketSweepTask.runOneIteration(context.sweepableBucket()))
                .isEqualTo(0);
        verifyNoInteractions(completionListener, sweepQueueReader, sweepQueueDeleter, sweepQueueCleaner);
    }

    @ParameterizedTest
    @MethodSource("allSweepBuckets")
    public void returnsEarlyIfSweepTimestampIsEqualToPastBucketProgress(SweepBucketTestContext context) {
        when(sweepTimestampSupplier.getAsLong()).thenReturn(context.startTimestampInclusive() + 7L);
        when(bucketProgressStore.getBucketProgress(context.bucket()))
                .thenReturn(Optional.of(BucketProgress.createForTimestampProgress(7L)));

        assertThat(defaultSingleBucketSweepTask.runOneIteration(context.sweepableBucket()))
                .isEqualTo(0);
        verifyNoInteractions(completionListener, sweepQueueReader, sweepQueueDeleter, sweepQueueCleaner);
    }

    @ParameterizedTest
    @MethodSource("closedSweepBuckets")
    public void sweepsEmptyDeleteableBucketEntirelyAndDeletesBucket(SweepBucketTestContext context) {
        when(sweepTimestampSupplier.getAsLong()).thenReturn(context.endTimestampExclusive());
        when(sweepQueueReader.getNextBatchToSweep(
                        context.shardAndStrategy(),
                        context.startTimestampInclusive() - 1,
                        context.endTimestampExclusive()))
                .thenReturn(SweepBatchWithPartitionInfo.of(
                        SweepBatch.of(
                                ImmutableSet.of(),
                                DedicatedRows.of(ImmutableList.of()),
                                context.endTimestampExclusive() - 1),
                        ImmutableSet.of()));
        when(boundRetriever.getStrictUpperBoundForCompletelyClosedBuckets()).thenReturn(Long.MAX_VALUE);

        defaultSingleBucketSweepTask.runOneIteration(context.sweepableBucket());
        verify(sweepQueueDeleter).sweep(eq(ImmutableList.of()), eq(Sweeper.of(context.shardAndStrategy())));
        verify(sweepQueueCleaner).clean(eq(context.shardAndStrategy()),
                eq(ImmutableSet.of()),
                eq(context.endTimestampExclusive() - 1L),
                eq(DedicatedRows.of(ImmutableList.of())));
        verify(bucketProgressStore)
                .updateBucketProgressToAtLeast(
                        context.bucket(), context.completeProgressForBucket().orElseThrow());
        verify(completionListener).markBucketCompleteAndRemoveFromScheduling(context.bucket());
    }

    @ParameterizedTest
    @MethodSource("closedSweepBuckets")
    public void sweepsEmptyNonDeleteableBucketEntirelyAndDoesNotDeleteBucket(SweepBucketTestContext context) {
        when(sweepTimestampSupplier.getAsLong()).thenReturn(context.endTimestampExclusive());
        when(sweepQueueReader.getNextBatchToSweep(
                        context.shardAndStrategy(),
                        context.startTimestampInclusive() - 1,
                        context.endTimestampExclusive()))
                .thenReturn(SweepBatchWithPartitionInfo.of(
                        SweepBatch.of(
                                ImmutableSet.of(),
                                DedicatedRows.of(ImmutableList.of()),
                                context.endTimestampExclusive() - 1),
                        ImmutableSet.of()));
        when(boundRetriever.getStrictUpperBoundForCompletelyClosedBuckets()).thenReturn(context.bucketIdentifier());

        defaultSingleBucketSweepTask.runOneIteration(context.sweepableBucket());
        verify(sweepQueueDeleter).sweep(eq(ImmutableList.of()), eq(Sweeper.of(context.shardAndStrategy())));
        verify(bucketProgressStore)
                .updateBucketProgressToAtLeast(
                        context.bucket(), context.completeProgressForBucket().orElseThrow());
        verifyNoInteractions(completionListener);
    }

    @ParameterizedTest
    @MethodSource("allSweepBuckets")
    public void sweepsEmptyBucketUpToSweepTimestamp(SweepBucketTestContext context) {
        long sweepTimestamp = context.startTimestampInclusive() + MIN_BUCKET_SIZE - 1;

        when(sweepTimestampSupplier.getAsLong()).thenReturn(sweepTimestamp);
        when(sweepQueueReader.getNextBatchToSweep(
                        context.shardAndStrategy(), context.startTimestampInclusive() - 1, sweepTimestamp))
                .thenReturn(SweepBatchWithPartitionInfo.of(
                        SweepBatch.of(ImmutableSet.of(), DedicatedRows.of(ImmutableList.of()), sweepTimestamp - 1),
                        ImmutableSet.of()));

        defaultSingleBucketSweepTask.runOneIteration(context.sweepableBucket());
        verify(sweepQueueDeleter).sweep(eq(ImmutableList.of()), eq(Sweeper.of(context.shardAndStrategy())));
        verify(bucketProgressStore)
                .updateBucketProgressToAtLeast(
                        context.bucket(),
                        BucketProgress.createForTimestampProgress(
                                sweepTimestamp - 1 - context.startTimestampInclusive()));
        verifyNoInteractions(completionListener);
    }

    @ParameterizedTest
    @MethodSource("allSweepBuckets")
    @SuppressWarnings("unchecked") // ArgumentCaptor invocation on known type
    public void passesThroughCellsInSweepBatchToDeleterAndStoresPartialProgress(SweepBucketTestContext context) {
        long sweepTimestamp = Long.MAX_VALUE - 858319L; // arbitrary, but confirming pass through for open buckets
        when(sweepTimestampSupplier.getAsLong()).thenReturn(sweepTimestamp);
        Set<WriteInfo> writeInfoSet = ImmutableSet.of(
                WriteInfo.write(
                        TABLE_REFERENCE,
                        Cell.create(PtBytes.toBytes("row"), PtBytes.toBytes("column")),
                        context.startTimestampInclusive() + 5L),
                WriteInfo.write(
                        TABLE_REFERENCE,
                        Cell.create(PtBytes.toBytes("quack"), PtBytes.toBytes("moo")),
                        context.startTimestampInclusive() + 15L),
                WriteInfo.tombstone(
                        TABLE_REFERENCE,
                        Cell.create(PtBytes.toBytes("rip"), PtBytes.toBytes("memoriam")),
                        context.startTimestampInclusive() + 25L));
        when(sweepQueueReader.getNextBatchToSweep(
                        context.shardAndStrategy(),
                        context.startTimestampInclusive() - 1,
                        context.isBucketClosed() ? context.endTimestampExclusive() : sweepTimestamp))
                .thenReturn(SweepBatchWithPartitionInfo.of(
                        SweepBatch.of(
                                writeInfoSet,
                                DedicatedRows.of(ImmutableList.of()),
                                context.startTimestampInclusive() + 30L),
                        ImmutableSet.of(4L)));

        defaultSingleBucketSweepTask.runOneIteration(context.sweepableBucket());

        ArgumentCaptor<Collection<WriteInfo>> writeInfoCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(sweepQueueDeleter).sweep(writeInfoCaptor.capture(), eq(Sweeper.of(context.shardAndStrategy())));
        assertThat(writeInfoCaptor.getValue()).containsExactlyInAnyOrderElementsOf(writeInfoSet);
        verify(bucketProgressStore)
                .updateBucketProgressToAtLeast(context.bucket(), BucketProgress.createForTimestampProgress(30L));
        verifyNoInteractions(completionListener);
    }

    public static Stream<Arguments> allSweepBuckets() {
        return Streams.concat(closedSweepBuckets(), openSweepBuckets());
    }

    public static Stream<Arguments> closedSweepBuckets() {
        return Stream.of(
                Arguments.of(Named.of(
                        "closed conservative bucket",
                        SweepBucketTestContext.builder()
                                .bucketIdentifier(42)
                                .shardAndStrategy(ShardAndStrategy.conservative(35))
                                .startTimestampInclusive(SweepQueueUtils.minTsForCoarsePartition(1_345_796L))
                                .endTimestampExclusive(SweepQueueUtils.minTsForCoarsePartition(8_888_888L))
                                .build())),
                Arguments.of(Named.of(
                        "closed thorough bucket",
                        SweepBucketTestContext.builder()
                                .bucketIdentifier(3)
                                .shardAndStrategy(ShardAndStrategy.thorough(99))
                                .startTimestampInclusive(SweepQueueUtils.minTsForCoarsePartition(147_258_369L))
                                .endTimestampExclusive(SweepQueueUtils.minTsForCoarsePartition(741_852_963L))
                                .build())));
    }

    public static Stream<Arguments> openSweepBuckets() {
        return Stream.of(
                Arguments.of(Named.of(
                        "open conservative bucket",
                        SweepBucketTestContext.builder()
                                .bucketIdentifier(666)
                                .shardAndStrategy(ShardAndStrategy.conservative(161))
                                .startTimestampInclusive(1_000_000_000L)
                                .endTimestampExclusive(-1L)
                                .build())),
                Arguments.of(Named.of(
                        "open thorough bucket",
                        SweepBucketTestContext.builder()
                                .bucketIdentifier(16384)
                                .shardAndStrategy(ShardAndStrategy.thorough(255))
                                .startTimestampInclusive(16384L)
                                .endTimestampExclusive(-1L)
                                .build())));
    }

    @Value.Immutable
    public interface SweepBucketTestContext {
        ShardAndStrategy shardAndStrategy();

        long bucketIdentifier();

        long startTimestampInclusive();

        long endTimestampExclusive();

        @Value.Derived
        default boolean isBucketClosed() {
            return endTimestampExclusive() != -1L;
        }

        @Value.Derived
        default Bucket bucket() {
            return Bucket.of(shardAndStrategy(), bucketIdentifier());
        }

        @Value.Derived
        default SweepableBucket sweepableBucket() {
            return SweepableBucket.of(bucket(), TimestampRange.of(startTimestampInclusive(), endTimestampExclusive()));
        }

        @Value.Derived
        default Optional<BucketProgress> completeProgressForBucket() {
            if (endTimestampExclusive() == -1) {
                return Optional.empty();
            } else {
                return Optional.of(BucketProgress.createForTimestampProgress(
                        endTimestampExclusive() - startTimestampInclusive() - 1));
            }
        }

        @Value.Check
        default void checkBucketIsAtLeastMinimumSize() {
            Preconditions.checkState(
                    endTimestampExclusive() == -1
                            || endTimestampExclusive() - startTimestampInclusive() + 1 >= MIN_BUCKET_SIZE,
                    "Buckets must either be closed, or consist of at least MIN_BUCKET_SIZE timestamps so that"
                            + " interesting assertions on them may be performed.");
        }

        static ImmutableSweepBucketTestContext.Builder builder() {
            return ImmutableSweepBucketTestContext.builder();
        }
    }
}
