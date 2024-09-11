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

import static com.palantir.atlasdb.sweep.asts.bucketingthings.DefaultBucketCloseTimestampCalculator.MAX_BUCKET_SIZE_FOR_NON_PUNCHER_CLOSE;
import static com.palantir.atlasdb.sweep.asts.bucketingthings.DefaultBucketCloseTimestampCalculator.MIN_BUCKET_SIZE_FOR_NON_PUNCHER_CLOSE;
import static com.palantir.atlasdb.sweep.asts.bucketingthings.DefaultBucketCloseTimestampCalculator.TIME_GAP_BETWEEN_BUCKET_START_AND_END;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.cleaner.PuncherStore;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class DefaultBucketCloseTimestampCalculatorTest {
    private final AtomicLong freshTimestamp = new AtomicLong(0);
    private final FakeClock clock = new FakeClock();

    @Mock
    private PuncherStore puncherStore;

    private DefaultBucketCloseTimestampCalculator bucketCloseTimestampCalculator;

    @BeforeEach
    public void setup() {
        bucketCloseTimestampCalculator =
                new DefaultBucketCloseTimestampCalculator(puncherStore, freshTimestamp::get, clock);
    }

    @Test
    public void returnsEmptyIfSufficientTimeHasNotPassedSinceStartTimestamp() {
        int startTimestamp = 18; // Arbitrarily chosen.
        when(puncherStore.getMillisForTimestamp(startTimestamp)).thenReturn(clock.millis());
        OptionalLong maybeEndTimestamp = bucketCloseTimestampCalculator.getBucketCloseTimestamp(startTimestamp);
        assertThat(maybeEndTimestamp).isEmpty();
    }

    @Test
    public void
            returnsLogicalTimestampSufficientTimeAfterStartTimestampIfTenMinutesHasPassedAndLogicalTimestampAheadOfStart() {
        int startTimestamp = 123;
        when(puncherStore.getMillisForTimestamp(startTimestamp)).thenReturn(clock.millis());
        clock.advance(TIME_GAP_BETWEEN_BUCKET_START_AND_END);
        when(puncherStore.get(clock.millis())).thenReturn(153L);

        OptionalLong maybeEndTimestamp = bucketCloseTimestampCalculator.getBucketCloseTimestamp(startTimestamp);
        assertThat(maybeEndTimestamp).hasValue(153L);
    }

    @ParameterizedTest
    @ValueSource(longs = {2300, 2315}) // less than, and equal to.
    // This is to test what happens when the puncher store returns a timestamp less than (or equal to) the start
    // timestamp
    // In both of these cases, we should not use the punch table result, but instead fallback to the relevant algorithm.
    public void
            returnsEmptyIfSufficientTimeHasPassedPuncherTimestampBeforeStartAndLatestFreshTimestampNotFarEnoughAhead(
                    long puncherTimestamp) {
        int startTimestamp = 2315;
        when(puncherStore.getMillisForTimestamp(startTimestamp)).thenReturn(clock.millis());
        clock.advance(TIME_GAP_BETWEEN_BUCKET_START_AND_END);
        when(puncherStore.get(clock.millis())).thenReturn(puncherTimestamp);

        freshTimestamp.set(MIN_BUCKET_SIZE_FOR_NON_PUNCHER_CLOSE - 1 + startTimestamp);

        OptionalLong maybeEndTimestamp = bucketCloseTimestampCalculator.getBucketCloseTimestamp(startTimestamp);
        assertThat(maybeEndTimestamp).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(longs = {123, 35124})
    public void
            returnsLatestFreshTimestampIfSufficientTimeHasPassedPuncherTimestampBeforeStartAndCalculatedTimestampFarEnoughAhead(
                    long puncherTimestamp) {
        int startTimestamp = 35124;
        when(puncherStore.getMillisForTimestamp(startTimestamp)).thenReturn(clock.millis());
        clock.advance(TIME_GAP_BETWEEN_BUCKET_START_AND_END);
        when(puncherStore.get(clock.millis())).thenReturn(puncherTimestamp);

        freshTimestamp.set(MIN_BUCKET_SIZE_FOR_NON_PUNCHER_CLOSE + 1 + startTimestamp);

        OptionalLong maybeEndTimestamp = bucketCloseTimestampCalculator.getBucketCloseTimestamp(startTimestamp);
        assertThat(maybeEndTimestamp).hasValue(freshTimestamp.get());
    }

    @ParameterizedTest
    @ValueSource(longs = {98, 100})
    public void returnsCappedTimestampIfPuncherTimestampBeforeStartAndLatestFreshTimestampIsTooFarAhead(
            long puncherTimestamp) {
        int startTimestamp = 100;
        when(puncherStore.getMillisForTimestamp(startTimestamp)).thenReturn(clock.millis());
        clock.advance(TIME_GAP_BETWEEN_BUCKET_START_AND_END);
        when(puncherStore.get(clock.millis())).thenReturn(puncherTimestamp);

        freshTimestamp.set(2 * MAX_BUCKET_SIZE_FOR_NON_PUNCHER_CLOSE);

        OptionalLong maybeEndTimestamp = bucketCloseTimestampCalculator.getBucketCloseTimestamp(startTimestamp);
        assertThat(maybeEndTimestamp).hasValue(startTimestamp + MAX_BUCKET_SIZE_FOR_NON_PUNCHER_CLOSE);
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
