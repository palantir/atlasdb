/**
 * Copyright 2017 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.timestamp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.assertj.core.api.Assertions;

import com.palantir.timestamp.TimestampMigrationService;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;

public class TimestampServiceTests {
    private static final long ONE_MILLION = 1000 * 1000;
    private static final long TWO_MILLION = 2 * ONE_MILLION;

    private static ExecutorService executor = Executors.newFixedThreadPool(16);

    private TimestampServiceTests() {

    }

    public static void timestampsAreReturnedInOrder(TimestampService timestampService) {
        List<Long> timestamps = new ArrayList<>();

        timestamps.add(timestampService.getFreshTimestamp());
        timestamps.add(timestampService.getFreshTimestamp());

        assertThat(timestamps.get(0), lessThan(timestamps.get(1)));
    }

    public static void canRequestTimestampRangeWithGetFreshTimestamps(TimestampService timestampService) {
        int expectedNumTimestamps = 5;
        TimestampRange range = timestampService.getFreshTimestamps(expectedNumTimestamps);

        long actualNumTimestamps = range.getUpperBound() - range.getLowerBound() + 1;
        assertThat(
                String.format("Expected %d timestamps, got %d timestamps. (The returned range was: %d-%d)",
                        expectedNumTimestamps, actualNumTimestamps, range.getLowerBound(), range.getUpperBound()),
                (int) actualNumTimestamps,
                is(both(greaterThanOrEqualTo(1)).and(lessThanOrEqualTo(expectedNumTimestamps))));
    }

    public static void timestampRangesAreReturnedInNonOverlappingOrder(TimestampService timestampService) {
        List<TimestampRange> timestampRanges = new ArrayList<>();

        timestampRanges.add(timestampService.getFreshTimestamps(10));
        timestampRanges.add(timestampService.getFreshTimestamps(10));

        long firstUpperBound = timestampRanges.get(0).getUpperBound();
        long secondLowerBound = timestampRanges.get(1).getLowerBound();

        assertThat(firstUpperBound, is(lessThan(secondLowerBound)));
    }

    public static void canRequestMoreTimestampsThanAreAllocatedAtOnce(TimestampService timestampService) {
        for (int i = 0; i < ONE_MILLION / 1000; i++) {
            timestampService.getFreshTimestamps(1000);
        }

        assertThat(timestampService.getFreshTimestamp(), is(ONE_MILLION + 1));
    }

    public static void willNotHandOutTimestampsEarlierThanAFastForward(
            TimestampMigrationService timestampMigrationService, TimestampService timestampService) {
        timestampMigrationService.fastForwardTimestamp(TWO_MILLION);

        assertThat(timestampService.getFreshTimestamp(), is(greaterThan(TWO_MILLION)));
    }

    public static void willDoNothingWhenFastForwardToEarlierTimestamp(
            TimestampMigrationService timestampMigrationService,
            TimestampService timestampService) {
        timestampMigrationService.fastForwardTimestamp(TWO_MILLION);
        long ts1 = timestampService.getFreshTimestamp();
        timestampMigrationService.fastForwardTimestamp(ONE_MILLION);
        long ts2 = timestampService.getFreshTimestamp();
        assertThat(ts2, greaterThan(TWO_MILLION));
        assertThat(ts2, greaterThan(ts1));
    }

    public static void canReturnManyUniqueTimestampsInParallel(TimestampService timestampService)
            throws InterruptedException, TimeoutException {
        Set<Long> uniqueTimestamps = new ConcurrentSkipListSet<>();

        repeat(TWO_MILLION, () -> uniqueTimestamps.add(timestampService.getFreshTimestamp()));

        assertThat(uniqueTimestamps.size(), is((int) TWO_MILLION));
    }

    private static void repeat(long count, Runnable task) throws InterruptedException, TimeoutException {
        for (int i = 0; i < count; i++) {
            executor.submit(task);
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);

        if (!executor.isTerminated()) {
            throw new TimeoutException("Timed out waiting for the executor to terminate");
        }
    }

    public static void shouldThrowIfRequestingNegativeNumbersOfTimestamps(TimestampService timestampService) {
        Assertions.assertThatThrownBy(() -> timestampService.getFreshTimestamps(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    public static void shouldThrowIfRequestingZeroTimestamps(TimestampService timestampService) {
        Assertions.assertThatThrownBy(() -> timestampService.getFreshTimestamps(0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
