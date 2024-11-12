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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.sweep.asts.bucketingthings.SweepBucketPointerTable;
import com.palantir.atlasdb.sweep.asts.bucketingthings.SweepBucketRecordsTable;
import com.palantir.atlasdb.sweep.asts.bucketingthings.SweepBucketsTable;
import com.palantir.atlasdb.sweep.asts.progress.BucketProgress;
import com.palantir.atlasdb.sweep.asts.progress.BucketProgressStore;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.SweepQueueProgressUpdater;
import com.palantir.atlasdb.sweep.queue.SweepQueueUtils;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
    private SweepBucketsTable sweepBucketsTable;

    @Mock
    private SweepBucketPointerTable sweepBucketPointerTable;

    private DefaultShardProgressUpdater shardProgressUpdater;

    @BeforeEach
    public void setUp() {
        shardProgressUpdater = new DefaultShardProgressUpdater(
                bucketProgressStore,
                sweepQueueProgressUpdater,
                recordsTable,
                sweepBucketsTable,
                sweepBucketPointerTable);
    }

    @ParameterizedTest
    @MethodSource("buckets")
    public void doesNotUpdateProgressOnUnstartedOpenBucket(Bucket bucket) {
        when(sweepBucketPointerTable.getStartingBucketsForShards(ImmutableSet.of(bucket.shardAndStrategy())))
                .thenReturn(ImmutableSet.of(bucket));
        when(bucketProgressStore.getBucketProgress(bucket)).thenReturn(Optional.empty());
        when(recordsTable.getTimestampRangeRecord(bucket.bucketIdentifier())).thenReturn(Optional.empty());

        TimestampRange timestampRange = TimestampRange.openBucket(SweepQueueUtils.minTsForCoarsePartition(3));
        when(sweepBucketsTable.getSweepableBucket(bucket))
                .thenReturn(Optional.of(SweepableBucket.of(bucket, timestampRange)));

        shardProgressUpdater.updateProgress(bucket.shardAndStrategy());

        verify(sweepBucketPointerTable).updateStartingBucketForShardAndStrategy(bucket);
        verify(sweepQueueProgressUpdater)
                .progressTo(bucket.shardAndStrategy(), SweepQueueUtils.minTsForCoarsePartition(3) - 1L);
        verify(bucketProgressStore, never()).deleteBucketProgress(any());
    }

    @ParameterizedTest
    @MethodSource("buckets")
    public void doesNotUpdateProgressOnUnstartedClosedBucket(Bucket bucket) {
        when(sweepBucketPointerTable.getStartingBucketsForShards(ImmutableSet.of(bucket.shardAndStrategy())))
                .thenReturn(ImmutableSet.of(bucket));
        when(bucketProgressStore.getBucketProgress(bucket)).thenReturn(Optional.empty());
        TimestampRange timestampRange = TimestampRange.of(
                SweepQueueUtils.minTsForCoarsePartition(3), SweepQueueUtils.minTsForCoarsePartition(8));
        when(recordsTable.getTimestampRangeRecord(bucket.bucketIdentifier())).thenReturn(Optional.of(timestampRange));

        when(sweepBucketsTable.getSweepableBucket(bucket))
                .thenReturn(Optional.of(SweepableBucket.of(bucket, timestampRange)));

        shardProgressUpdater.updateProgress(bucket.shardAndStrategy());

        verify(sweepBucketPointerTable).updateStartingBucketForShardAndStrategy(bucket);
        verify(sweepQueueProgressUpdater)
                .progressTo(bucket.shardAndStrategy(), SweepQueueUtils.minTsForCoarsePartition(3) - 1L);
        verify(bucketProgressStore, never()).deleteBucketProgress(any());
    }

    @ParameterizedTest
    @MethodSource("buckets")
    public void throwsIfOpenBucketHasNoBucketEntryAndNoBucketsToReadPrior(Bucket bucket) {
        when(sweepBucketPointerTable.getStartingBucketsForShards(ImmutableSet.of(bucket.shardAndStrategy())))
                .thenReturn(ImmutableSet.of(bucket));
        when(bucketProgressStore.getBucketProgress(bucket)).thenReturn(Optional.empty());
        when(recordsTable.getTimestampRangeRecord(bucket.bucketIdentifier())).thenReturn(Optional.empty());
        when(sweepBucketsTable.getSweepableBucket(bucket)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> shardProgressUpdater.updateProgress(bucket.shardAndStrategy()));
    }

    @ParameterizedTest
    @MethodSource("sweepableBuckets")
    public void updatesProgressOnStartedButNotCompletedOpenBucket(SweepableBucket sweepableBucket) {
        Bucket bucket = sweepableBucket.bucket();
        when(sweepBucketPointerTable.getStartingBucketsForShards(ImmutableSet.of(bucket.shardAndStrategy())))
                .thenReturn(ImmutableSet.of(bucket));
        when(bucketProgressStore.getBucketProgress(bucket))
                .thenReturn(Optional.of(BucketProgress.createForTimestampProgress(1_234_567L)));
        when(recordsTable.getTimestampRangeRecord(bucket.bucketIdentifier())).thenReturn(Optional.empty());
        when(sweepBucketsTable.getSweepableBucket(bucket))
                .thenReturn(Optional.of(SweepableBucket.of(
                        bucket,
                        TimestampRange.openBucket(
                                sweepableBucket.timestampRange().startInclusive()))));

        shardProgressUpdater.updateProgress(bucket.shardAndStrategy());

        verify(sweepBucketPointerTable).updateStartingBucketForShardAndStrategy(bucket);
        verify(sweepQueueProgressUpdater)
                .progressTo(
                        bucket.shardAndStrategy(),
                        sweepableBucket.timestampRange().startInclusive() + 1_234_567L);
        verify(bucketProgressStore, never()).deleteBucketProgress(any());
    }

    @ParameterizedTest
    @MethodSource("sweepableBuckets")
    public void updatesProgressOnStartedButNotCompletedClosedBucket(SweepableBucket sweepableBucket) {
        Bucket bucket = sweepableBucket.bucket();
        when(sweepBucketPointerTable.getStartingBucketsForShards(ImmutableSet.of(bucket.shardAndStrategy())))
                .thenReturn(ImmutableSet.of(bucket));
        when(bucketProgressStore.getBucketProgress(bucket))
                .thenReturn(Optional.of(BucketProgress.createForTimestampProgress(1_234_567L)));
        when(recordsTable.getTimestampRangeRecord(bucket.bucketIdentifier()))
                .thenReturn(Optional.of(sweepableBucket.timestampRange()));
        when(sweepBucketsTable.getSweepableBucket(bucket)).thenReturn(Optional.of(sweepableBucket));

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
    public void progressesPastOneOrMoreCompletedBucketsAndStopsCorrectlyForPresentLastBucket(
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
        when(sweepBucketsTable.getSweepableBucket(Bucket.of(firstRawBucket.shardAndStrategy(), finalBucketIdentifier)))
                .thenReturn(Optional.of(SweepableBucket.of(
                        Bucket.of(firstRawBucket.shardAndStrategy(), finalBucketIdentifier),
                        finalBucketTimestampRange)));
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
    }

    @Test
    public void doesNotLoadPastMaxBucketsForIteration() {
        Bucket firstRawBucket = Bucket.of(ShardAndStrategy.conservative(0), 0L);
        SweepableBucket firstBucket =
                SweepableBucket.of(firstRawBucket, TimestampRange.of(0L, SweepQueueUtils.minTsForCoarsePartition(8L)));
        when(sweepBucketPointerTable.getStartingBucketsForShards(ImmutableSet.of(firstRawBucket.shardAndStrategy())))
                .thenReturn(ImmutableSet.of(firstRawBucket));
        setupBucketAsComplete(firstBucket);

        List<SweepableBucket> succeedingBuckets =
                getSucceedingBuckets(firstBucket, DefaultShardProgressUpdater.MAX_BUCKETS_TO_CHECK_PER_ITERATION - 2);

        // We have MAX_BUCKETS + 1 - the last succeeding bucket should not be used for calculating the progress
        // so we don't want to set any mocks based off it. If we do load it, we'll fail the test from the mock failing.
        succeedingBuckets.subList(0, succeedingBuckets.size() - 1).forEach(this::setupBucketAsComplete);

        shardProgressUpdater.updateProgress(firstRawBucket.shardAndStrategy());

        SweepableBucket lastLoadedBucket = succeedingBuckets.get(succeedingBuckets.size() - 2);
        verify(sweepBucketPointerTable)
                .updateStartingBucketForShardAndStrategy(Bucket.of(
                        firstRawBucket.shardAndStrategy(),
                        lastLoadedBucket.bucket().bucketIdentifier() + 1));
        verify(sweepQueueProgressUpdater)
                .progressTo(
                        firstRawBucket.shardAndStrategy(),
                        lastLoadedBucket.timestampRange().endExclusive() - 1);
    }

    @ParameterizedTest
    @MethodSource("bucketProbeParameters")
    public void progressesPastOneOrMoreCompletedBucketsAndStopsCorrectlyEvenWhenFinalBucketDoesNotExist(
            SweepableBucket firstBucket,
            long numAdditionalCompletedBuckets,
            Optional<BucketProgress> _progressOnFinalBucket) {
        Bucket firstRawBucket = firstBucket.bucket();
        when(sweepBucketPointerTable.getStartingBucketsForShards(ImmutableSet.of(firstRawBucket.shardAndStrategy())))
                .thenReturn(ImmutableSet.of(firstRawBucket));

        List<SweepableBucket> succeedingBuckets = getSucceedingBuckets(firstBucket, numAdditionalCompletedBuckets);
        List<SweepableBucket> allSuccessfulBuckets = ImmutableList.<SweepableBucket>builder()
                .add(firstBucket)
                .addAll(succeedingBuckets.subList(0, succeedingBuckets.size() - 1))
                .build();
        allSuccessfulBuckets.forEach(this::setupBucketAsComplete);

        long finalBucketIdentifier = firstRawBucket.bucketIdentifier() + numAdditionalCompletedBuckets;
        when(bucketProgressStore.getBucketProgress(Bucket.of(firstRawBucket.shardAndStrategy(), finalBucketIdentifier)))
                .thenReturn(Optional.empty());
        TimestampRange lastCompleteBucketTimestampRange =
                allSuccessfulBuckets.get(allSuccessfulBuckets.size() - 1).timestampRange();
        when(recordsTable.getTimestampRangeRecord(finalBucketIdentifier)).thenReturn(Optional.empty());
        when(sweepBucketsTable.getSweepableBucket(Bucket.of(firstRawBucket.shardAndStrategy(), finalBucketIdentifier)))
                .thenReturn(Optional.empty());
        shardProgressUpdater.updateProgress(firstRawBucket.shardAndStrategy());

        verify(sweepBucketPointerTable)
                .updateStartingBucketForShardAndStrategy(Bucket.of(
                        firstRawBucket.shardAndStrategy(), finalBucketIdentifier)); // all buckets before are complete
        verify(sweepQueueProgressUpdater)
                .progressTo(firstRawBucket.shardAndStrategy(), lastCompleteBucketTimestampRange.endExclusive() - 1);
    }

    private void setupBucketAsComplete(SweepableBucket sweepableBucket) {
        setupBucketRecord(sweepableBucket);
        when(bucketProgressStore.getBucketProgress(sweepableBucket.bucket()))
                .thenReturn(Optional.of(BucketProgress.createForTimestampProgress(
                        sweepableBucket.timestampRange().endExclusive()
                                - sweepableBucket.timestampRange().startInclusive()
                                - 1L)));
        when(sweepBucketsTable.getSweepableBucket(sweepableBucket.bucket())).thenReturn(Optional.of(sweepableBucket));
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
