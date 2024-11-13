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

import static com.palantir.atlasdb.sweep.asts.bucketingthings.DefaultBucketCloseTimestampCalculator.MIN_BUCKET_SIZE;
import static com.palantir.atlasdb.sweep.asts.bucketingthings.DefaultBucketCloseTimestampCalculator.TIME_GAP_BETWEEN_BUCKET_START_AND_END;
import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.cleaner.PuncherStore;
import com.palantir.atlasdb.sweep.queue.SweepQueueUtils;
import com.palantir.logsafe.SafeArg;
import com.palantir.refreshable.Refreshable;
import com.palantir.refreshable.SettableRefreshable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collection;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.eclipse.collections.impl.factory.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class DefaultBucketCloseTimestampCalculatorTest {
    private final AtomicLong freshTimestamp = new AtomicLong(0);
    private final SettableRefreshable<Long> maxNumberOfCoarsePartitionsPerBucketForNonPuncherClose =
            Refreshable.create(500L);
    private final FakeClock clock = new FakeClock();

    @Mock
    private PuncherStore puncherStore;

    private DefaultBucketCloseTimestampCalculator bucketCloseTimestampCalculator;

    @BeforeEach
    public void setup() {
        bucketCloseTimestampCalculator = new DefaultBucketCloseTimestampCalculator(
                puncherStore, freshTimestamp::get, maxNumberOfCoarsePartitionsPerBucketForNonPuncherClose, clock);
    }

    @Test
    public void throwsIfStartTimestampNotOnCoarsePartitionBoundary() {
        long startTimestamp = 18; // Arbitrarily chosen.
        assertThatLoggableExceptionThrownBy(
                        () -> bucketCloseTimestampCalculator.getBucketCloseTimestamp(startTimestamp))
                .hasLogMessage("startTimestamp must be on a coarse partition boundary")
                .hasExactlyArgs(SafeArg.of("startTimestamp", startTimestamp));
    }

    @Test
    public void
            returnsLogicalTimestampSufficientTimeAfterStartTimestampIfTenMinutesHasPassedAndLogicalTimestampAheadOfStart() {
        long startTimestamp = 123 * SweepQueueUtils.TS_COARSE_GRANULARITY;
        when(puncherStore.getMillisForTimestamp(startTimestamp)).thenReturn(clock.millis());
        clock.advance(TIME_GAP_BETWEEN_BUCKET_START_AND_END);

        long punchStoreTimestamp = 2 * SweepQueueUtils.TS_COARSE_GRANULARITY + startTimestamp;
        when(puncherStore.get(clock.millis())).thenReturn(punchStoreTimestamp);

        OptionalLong maybeEndTimestamp = bucketCloseTimestampCalculator.getBucketCloseTimestamp(startTimestamp);
        assertThat(maybeEndTimestamp).hasValue(punchStoreTimestamp);
    }

    @ParameterizedTest
    @ValueSource(
            longs = {
                2300 * SweepQueueUtils.TS_COARSE_GRANULARITY,
                2315 * SweepQueueUtils.TS_COARSE_GRANULARITY,
                2315 * SweepQueueUtils.TS_COARSE_GRANULARITY + 1,
                2316 * SweepQueueUtils.TS_COARSE_GRANULARITY - 1
            }) // less than, equal to, and insufficiently greater than
    // This is to test what happens when the puncher store returns a timestamp less than / equal / insufficiently
    // greater than the start timestamp
    // In all of these cases, we should not use the punch table result, but instead fallback to the relevant algorithm.
    public void
            returnsEmptyIfSufficientTimeHasPassedPuncherTimestampInsufficientlyFarAndLatestFreshTimestampNotFarEnoughAhead(
                    long puncherTimestamp) {
        long startTimestamp = 2315 * SweepQueueUtils.TS_COARSE_GRANULARITY;
        when(puncherStore.getMillisForTimestamp(startTimestamp)).thenReturn(clock.millis());
        clock.advance(TIME_GAP_BETWEEN_BUCKET_START_AND_END);
        when(puncherStore.get(clock.millis())).thenReturn(puncherTimestamp);

        freshTimestamp.set(MIN_BUCKET_SIZE - 1 + startTimestamp);

        OptionalLong maybeEndTimestamp = bucketCloseTimestampCalculator.getBucketCloseTimestamp(startTimestamp);
        assertThat(maybeEndTimestamp).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(
            longs = {
                123 * SweepQueueUtils.TS_COARSE_GRANULARITY,
                312 * SweepQueueUtils.TS_COARSE_GRANULARITY,
                312 * SweepQueueUtils.TS_COARSE_GRANULARITY + 1,
                313 * SweepQueueUtils.TS_COARSE_GRANULARITY - 1
            })
    public void
            returnsLatestClampedFreshTimestampIfSufficientTimeHasPassedPuncherTimestampInsufficientlyFarAndCalculatedTimestampFarEnoughAhead(
                    long puncherTimestamp) {
        long startTimestamp = 312 * SweepQueueUtils.TS_COARSE_GRANULARITY;
        when(puncherStore.getMillisForTimestamp(startTimestamp)).thenReturn(clock.millis());
        clock.advance(TIME_GAP_BETWEEN_BUCKET_START_AND_END);
        when(puncherStore.get(clock.millis())).thenReturn(puncherTimestamp);

        freshTimestamp.set(11 * SweepQueueUtils.TS_COARSE_GRANULARITY + startTimestamp);

        OptionalLong maybeEndTimestamp = bucketCloseTimestampCalculator.getBucketCloseTimestamp(startTimestamp);
        assertThat(maybeEndTimestamp).hasValue(freshTimestamp.get());
    }

    @ParameterizedTest
    @MethodSource("puncherTimestampAndMaxCoarsePartitions")
    public void returnsClampedAndCappedTimestampIfPuncherTimestampInsufficientlyFarAndLatestFreshTimestampIsTooFarAhead(
            long puncherTimestamp, long maxCoarsePartitions) {
        long startTimestamp = 100 * SweepQueueUtils.TS_COARSE_GRANULARITY;
        when(puncherStore.getMillisForTimestamp(startTimestamp)).thenReturn(clock.millis());
        clock.advance(TIME_GAP_BETWEEN_BUCKET_START_AND_END);
        when(puncherStore.get(clock.millis())).thenReturn(puncherTimestamp);
        maxNumberOfCoarsePartitionsPerBucketForNonPuncherClose.update(maxCoarsePartitions);
        long maxBucketSize = maxCoarsePartitions * SweepQueueUtils.TS_COARSE_GRANULARITY;
        freshTimestamp.set(2 * maxBucketSize + startTimestamp);

        OptionalLong maybeEndTimestamp = bucketCloseTimestampCalculator.getBucketCloseTimestamp(startTimestamp);
        assertThat(maybeEndTimestamp).hasValue(maxBucketSize + startTimestamp);
    }

    @ParameterizedTest
    @ValueSource(longs = {0, -1})
    public void throwsIfMaxBucketSizeIsNonPositiveAndPuncherTimestampInsufficientlyFar(long maxCoarsePartitions) {
        long startTimestamp = 0;
        when(puncherStore.getMillisForTimestamp(startTimestamp)).thenReturn(clock.millis());
        clock.advance(TIME_GAP_BETWEEN_BUCKET_START_AND_END);
        when(puncherStore.get(clock.millis())).thenReturn(startTimestamp);

        maxNumberOfCoarsePartitionsPerBucketForNonPuncherClose.update(maxCoarsePartitions);
        freshTimestamp.set(SweepQueueUtils.TS_COARSE_GRANULARITY + startTimestamp);

        assertThatLoggableExceptionThrownBy(
                        () -> bucketCloseTimestampCalculator.getBucketCloseTimestamp(startTimestamp))
                .hasLogMessage("max coarse partitions must be positive")
                .hasExactlyArgs(SafeArg.of("maxCoarsePartitions", maxCoarsePartitions));
    }

    @Test
    public void loadsLatestMaxCoarsePartitions() {
        long startTimestamp = 0;
        when(puncherStore.getMillisForTimestamp(startTimestamp)).thenReturn(clock.millis());
        clock.advance(TIME_GAP_BETWEEN_BUCKET_START_AND_END);
        when(puncherStore.get(clock.millis())).thenReturn(startTimestamp);

        maxNumberOfCoarsePartitionsPerBucketForNonPuncherClose.update(1L);
        freshTimestamp.set(SweepQueueUtils.TS_COARSE_GRANULARITY * 10 + startTimestamp);

        OptionalLong maybeEndTimestamp = bucketCloseTimestampCalculator.getBucketCloseTimestamp(startTimestamp);
        assertThat(maybeEndTimestamp).hasValue(SweepQueueUtils.TS_COARSE_GRANULARITY + startTimestamp);

        maxNumberOfCoarsePartitionsPerBucketForNonPuncherClose.update(6L);
        OptionalLong maybeEndTimestamp2 = bucketCloseTimestampCalculator.getBucketCloseTimestamp(startTimestamp);
        assertThat(maybeEndTimestamp2).hasValue(6 * SweepQueueUtils.TS_COARSE_GRANULARITY + startTimestamp);
    }

    private static Collection<Arguments> puncherTimestampAndMaxCoarsePartitions() {
        Set<Long> puncherTimestamps = Set.of(
                98 * SweepQueueUtils.TS_COARSE_GRANULARITY,
                100 * SweepQueueUtils.TS_COARSE_GRANULARITY,
                100 * SweepQueueUtils.TS_COARSE_GRANULARITY + 1,
                101 * SweepQueueUtils.TS_COARSE_GRANULARITY - 1);
        Set<Long> maxCoarsePartitions = Set.of(1L, 2L, 500L);
        return Sets.cartesianProduct(puncherTimestamps, maxCoarsePartitions)
                .collect(pair -> Arguments.of(pair.getOne(), pair.getTwo()))
                .toList();
    }

    // TODO(mdaudali): Extract this into its own class if we end up needing this elsewhere.
    private static final class FakeClock extends Clock {
        public static final Instant BASE = Instant.parse("1999-04-20T20:15:00Z");

        private final ZoneId zoneId;
        private final AtomicReference<Instant> currentTime;

        FakeClock(AtomicReference<Instant> currentTime, ZoneId zoneId) {
            this.currentTime = currentTime;
            this.zoneId = zoneId;
        }

        FakeClock() {
            this(new AtomicReference<>(BASE), ZoneId.of("Europe/London"));
        }

        @Override
        public ZoneId getZone() {
            return zoneId;
        }

        @Override
        public Clock withZone(ZoneId _zone) {
            return new FakeClock(currentTime, zoneId);
        }

        @Override
        public Instant instant() {
            return currentTime.get();
        }

        public FakeClock advance(Duration difference) {
            currentTime.getAndUpdate(current -> current.plus(difference));
            return this;
        }
    }
}
