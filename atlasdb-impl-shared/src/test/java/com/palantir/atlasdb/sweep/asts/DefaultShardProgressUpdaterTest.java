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

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.sweep.asts.bucketingthings.SweepBucketPointerTable;
import com.palantir.atlasdb.sweep.asts.bucketingthings.SweepBucketRecordsTable;
import com.palantir.atlasdb.sweep.asts.progress.BucketProgress;
import com.palantir.atlasdb.sweep.asts.progress.BucketProgressStore;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.SweepQueueProgressUpdater;
import com.palantir.atlasdb.sweep.queue.SweepQueueUtils;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DefaultShardProgressUpdaterTest {
    @Mock
    private BucketProgressStore bucketProgressStore;

    @Mock
    private SweepQueueProgressUpdater sweepQueueProgressUpdater;

    @Mock
    private SweepBucketRecordsTable recordsTable;

    @Mock
    private SweepBucketPointerTable sweepBucketPointerTable;

    private DefaultShardProgressUpdater shardProgressUpdater;

    @BeforeEach
    public void setUp() {
        shardProgressUpdater = new DefaultShardProgressUpdater(
                bucketProgressStore, sweepQueueProgressUpdater, recordsTable, sweepBucketPointerTable);
    }

    @ParameterizedTest
    @MethodSource("buckets")
    public void throwsExceptionOnAbsenceOfTimestampRangeRecords(Bucket bucket) {
        when(sweepBucketPointerTable.getStartingBucketsForShards(ImmutableSet.of(bucket.shardAndStrategy())))
                .thenReturn(ImmutableSet.of(bucket));
        when(recordsTable.getTimestampRangeRecord(bucket.bucketIdentifier())).thenReturn(Optional.empty());

        assertThatLoggableExceptionThrownBy(() -> shardProgressUpdater.updateProgress(bucket.shardAndStrategy()))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasLogMessage("Timestamp range record not found. If this has happened for bucket 0, this is possible"
                        + " when autoscaling sweep is initializing itself. Otherwise, this is potentially indicative of"
                        + " a bug in auto-scaling sweep. In either case, we will retry.")
                .hasExactlyArgs(SafeArg.of("queriedBucket", bucket.bucketIdentifier()));

        verify(sweepBucketPointerTable, never()).updateStartingBucketForShardAndStrategy(bucket);
        verify(sweepQueueProgressUpdater, never()).progressTo(eq(bucket.shardAndStrategy()), anyLong());
        verify(bucketProgressStore, never()).deleteBucketProgress(any());
    }

    @ParameterizedTest
    @MethodSource("buckets")
    public void doesNotUpdateProgressOnUnstartedBucket(Bucket bucket) {
        when(sweepBucketPointerTable.getStartingBucketsForShards(ImmutableSet.of(bucket.shardAndStrategy())))
                .thenReturn(ImmutableSet.of(bucket));
        when(bucketProgressStore.getBucketProgress(bucket)).thenReturn(Optional.empty());
        when(recordsTable.getTimestampRangeRecord(bucket.bucketIdentifier()))
                .thenReturn(Optional.of(TimestampRange.of(
                        SweepQueueUtils.minTsForCoarsePartition(3), SweepQueueUtils.minTsForCoarsePartition(8))));

        shardProgressUpdater.updateProgress(bucket.shardAndStrategy());

        verify(sweepBucketPointerTable).updateStartingBucketForShardAndStrategy(bucket);
        verify(sweepQueueProgressUpdater)
                .progressTo(bucket.shardAndStrategy(), SweepQueueUtils.minTsForCoarsePartition(3) - 1L);
        verify(bucketProgressStore, never()).deleteBucketProgress(any());
    }

    @ParameterizedTest
    @MethodSource("sweepableBuckets")
    public void updatesProgressOnStartedButNotCompletedBucket(SweepableBucket sweepableBucket) {
        Bucket bucket = sweepableBucket.bucket();
        when(sweepBucketPointerTable.getStartingBucketsForShards(ImmutableSet.of(bucket.shardAndStrategy())))
                .thenReturn(ImmutableSet.of(bucket));
        when(bucketProgressStore.getBucketProgress(bucket))
                .thenReturn(Optional.of(BucketProgress.createForTimestampProgress(1_234_567L)));
        when(recordsTable.getTimestampRangeRecord(bucket.bucketIdentifier()))
                .thenReturn(Optional.of(sweepableBucket.timestampRange()));

        shardProgressUpdater.updateProgress(bucket.shardAndStrategy());

        verify(sweepBucketPointerTable).updateStartingBucketForShardAndStrategy(bucket);
        verify(sweepQueueProgressUpdater)
                .progressTo(
                        bucket.shardAndStrategy(),
                        sweepableBucket.timestampRange().startInclusive() + 1_234_567L);
        verify(bucketProgressStore, never()).deleteBucketProgress(any());
    }

    @ParameterizedTest
    @MethodSource("bucketProbeParameters")
    public void progressesPastOneOrMoreCompletedBucketsAndStopsCorrectly(
            SweepableBucket firstBucket,
            long numAdditionalCompletedBuckets,
            Optional<BucketProgress> progressOnFinalBucket) {
        Bucket firstRawBucket = firstBucket.bucket();
        when(sweepBucketPointerTable.getStartingBucketsForShards(ImmutableSet.of(firstRawBucket.shardAndStrategy())))
                .thenReturn(ImmutableSet.of(firstRawBucket));
        setupBucketAsComplete(firstBucket);

        List<SweepableBucket> succeedingBuckets = getSucceedingBuckets(firstBucket, numAdditionalCompletedBuckets);
        succeedingBuckets.forEach(this::setupBucketAsComplete);

        long finalBucketIdentifier = firstRawBucket.bucketIdentifier() + numAdditionalCompletedBuckets;
        when(bucketProgressStore.getBucketProgress(Bucket.of(firstRawBucket.shardAndStrategy(), finalBucketIdentifier)))
                .thenReturn(progressOnFinalBucket);
        TimestampRange lastCompleteBucketTimestampRange =
                succeedingBuckets.get(succeedingBuckets.size() - 1).timestampRange();
        TimestampRange finalBucketTimestampRange = TimestampRange.of(
                lastCompleteBucketTimestampRange.endExclusive(),
                lastCompleteBucketTimestampRange.endExclusive() + SweepQueueUtils.TS_COARSE_GRANULARITY);
        when(recordsTable.getTimestampRangeRecord(finalBucketIdentifier))
                .thenReturn(Optional.of(finalBucketTimestampRange));

        shardProgressUpdater.updateProgress(firstRawBucket.shardAndStrategy());

        verify(sweepBucketPointerTable)
                .updateStartingBucketForShardAndStrategy(
                        Bucket.of(firstRawBucket.shardAndStrategy(), finalBucketIdentifier));
        verify(sweepQueueProgressUpdater)
                .progressTo(
                        firstRawBucket.shardAndStrategy(),
                        finalBucketTimestampRange.startInclusive()
                                + progressOnFinalBucket
                                        .map(BucketProgress::timestampProgress)
                                        .orElse(-1L));

        for (long bucketIdentifier = firstBucket.bucket().bucketIdentifier();
                bucketIdentifier < finalBucketIdentifier;
                bucketIdentifier++) {
            verify(bucketProgressStore)
                    .deleteBucketProgress(Bucket.of(firstRawBucket.shardAndStrategy(), bucketIdentifier));
        }
    }

    private void setupBucketAsComplete(SweepableBucket sweepableBucket) {
        setupBucketRecord(sweepableBucket);
        when(bucketProgressStore.getBucketProgress(sweepableBucket.bucket()))
                .thenReturn(Optional.of(BucketProgress.createForTimestampProgress(
                        sweepableBucket.timestampRange().endExclusive()
                                - sweepableBucket.timestampRange().startInclusive()
                                - 1L)));
    }

    // Creates a list of sweepable buckets following the provided bucket, each with a range of TS_COARSE_GRANULARITY
    // timestamps, and sequentially increasing bucket identifiers.
    private static List<SweepableBucket> getSucceedingBuckets(SweepableBucket bucket, long numAdditionalBuckets) {
        return LongStream.rangeClosed(1, numAdditionalBuckets)
                .mapToObj(offset -> SweepableBucket.of(
                        Bucket.of(
                                bucket.bucket().shardAndStrategy(),
                                bucket.bucket().bucketIdentifier() + offset),
                        TimestampRange.of(
                                bucket.timestampRange().endExclusive()
                                        + (offset - 1) * SweepQueueUtils.TS_COARSE_GRANULARITY,
                                bucket.timestampRange().endExclusive()
                                        + offset * SweepQueueUtils.TS_COARSE_GRANULARITY)))
                .collect(Collectors.toList());
    }

    private void setupBucketRecord(SweepableBucket sweepableBucket) {
        when(recordsTable.getTimestampRangeRecord(sweepableBucket.bucket().bucketIdentifier()))
                .thenReturn(Optional.of(sweepableBucket.timestampRange()));
    }

    static Stream<Bucket> buckets() {
        return sweepableBuckets().map(SweepableBucket::bucket);
    }

    static Stream<SweepableBucket> sweepableBuckets() {
        return Stream.of(
                SweepableBucket.of(
                        Bucket.of(ShardAndStrategy.conservative(0), 0L),
                        TimestampRange.of(0L, SweepQueueUtils.minTsForCoarsePartition(8L))),
                SweepableBucket.of(
                        Bucket.of(ShardAndStrategy.conservative(189), 458L),
                        TimestampRange.of(
                                SweepQueueUtils.minTsForCoarsePartition(555L),
                                SweepQueueUtils.minTsForCoarsePartition(557L))),
                SweepableBucket.of(
                        Bucket.of(ShardAndStrategy.thorough(43), 227L),
                        TimestampRange.of(
                                SweepQueueUtils.minTsForCoarsePartition(1_111L),
                                SweepQueueUtils.minTsForCoarsePartition(1_337L))),
                SweepableBucket.of(
                        Bucket.of(ShardAndStrategy.thorough(188), 515L),
                        TimestampRange.of(
                                SweepQueueUtils.minTsForCoarsePartition(4_212L),
                                SweepQueueUtils.minTsForCoarsePartition(4_312L))),
                SweepableBucket.of(
                        Bucket.of(ShardAndStrategy.nonSweepable(), 1_888L),
                        TimestampRange.of(
                                SweepQueueUtils.minTsForCoarsePartition(4_212L),
                                SweepQueueUtils.minTsForCoarsePartition(4_312L))));
    }

    static Stream<Optional<BucketProgress>> incompleteProgressStates() {
        Stream<BucketProgress> presentProgresses = Stream.of(
                BucketProgress.INITIAL_PROGRESS,
                BucketProgress.createForTimestampProgress(0L),
                // Note: TS_COARSE_GRANULARITY - 1 would be complete for a minimally sized bucket, so we can't use that.
                BucketProgress.createForTimestampProgress(SweepQueueUtils.TS_COARSE_GRANULARITY - 2),
                BucketProgress.builder()
                        .timestampProgress(8_888_888L)
                        .cellProgressForNextTimestamp(1_234_567L)
                        .build(),
                BucketProgress.builder()
                        .timestampProgress(-1L)
                        .cellProgressForNextTimestamp(4_321L)
                        .build());
        return Stream.concat(
                presentProgresses.map(Optional::of),
                // Empty progress is to be interpreted as unstarted; hence this is an incomplete state.
                Stream.of(Optional.empty()));
    }

    static Stream<Arguments> bucketProbeParameters() {
        Set<Long> additionalBuckets =
                ImmutableSet.of(1L, 3L, 5L, DefaultShardProgressUpdater.MAX_BUCKETS_TO_CHECK_PER_ITERATION - 1);
        return Sets.cartesianProduct(
                        sweepableBuckets().collect(Collectors.toSet()),
                        additionalBuckets,
                        incompleteProgressStates().collect(Collectors.toSet()))
                .stream()
                .map(args -> Arguments.of(args.toArray()));
    }
}
