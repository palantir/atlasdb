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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.assertj.core.api.Assertions;

import com.palantir.timestamp.TimestampMigrationService;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;

public class TimestampServiceTests {
    private static final long ONE_MILLION = 1000 * 1000;
    private static final long TWO_MILLION = 2 * ONE_MILLION;

    private TimestampServiceTests() {

    }

    public static void timestampsAreReturnedInOrder(TimestampService timestampService) {
        List<Long> timestamps = new ArrayList<>();

        timestamps.add(timestampService.getFreshTimestamp());
        timestamps.add(timestampService.getFreshTimestamp());

        Assertions.assertThat(timestamps.get(0)).isLessThan(timestamps.get(1));
    }

    public static void canRequestTimestampRangeWithGetFreshTimestamps(TimestampService timestampService) {
        int expectedNumTimestamps = 5;
        TimestampRange range = timestampService.getFreshTimestamps(expectedNumTimestamps);

        long actualNumTimestamps = range.getUpperBound() - range.getLowerBound() + 1;

        Assertions.assertThat((int) actualNumTimestamps)
                .withFailMessage("Expected %d timestamps, got %d timestamps. (The returned range was: %d-%d)",
                        expectedNumTimestamps, actualNumTimestamps, range.getLowerBound(), range.getUpperBound())
                .isGreaterThanOrEqualTo(1)
                .isLessThanOrEqualTo(expectedNumTimestamps);
    }

    public static void timestampRangesAreReturnedInNonOverlappingOrder(TimestampService timestampService) {
        List<TimestampRange> timestampRanges = new ArrayList<>();

        timestampRanges.add(timestampService.getFreshTimestamps(10));
        timestampRanges.add(timestampService.getFreshTimestamps(10));

        long firstUpperBound = timestampRanges.get(0).getUpperBound();
        long secondLowerBound = timestampRanges.get(1).getLowerBound();

        Assertions.assertThat(firstUpperBound).isLessThan(secondLowerBound);
    }

    public static void canRequestMoreTimestampsThanAreAllocatedAtOnce(TimestampService timestampService) {
        for (int i = 0; i < ONE_MILLION / 1000; i++) {
            timestampService.getFreshTimestamps(1000);
        }

        Assertions.assertThat(timestampService.getFreshTimestamp()).isEqualTo(ONE_MILLION + 1);
    }

    public static void willNotHandOutTimestampsEarlierThanAFastForward(
            TimestampMigrationService timestampMigrationService, TimestampService timestampService) {
        timestampMigrationService.fastForwardTimestamp(TWO_MILLION);

        Assertions.assertThat(timestampService.getFreshTimestamp()).isGreaterThan(TWO_MILLION);
    }

    public static void willDoNothingWhenFastForwardToEarlierTimestamp(
            TimestampMigrationService timestampMigrationService,
            TimestampService timestampService) {
        timestampMigrationService.fastForwardTimestamp(TWO_MILLION);
        long ts1 = timestampService.getFreshTimestamp();
        timestampMigrationService.fastForwardTimestamp(ONE_MILLION);
        long ts2 = timestampService.getFreshTimestamp();
        Assertions.assertThat(ts2).isGreaterThan(TWO_MILLION);
        Assertions.assertThat(ts2).isGreaterThan(ts1);
    }

    public static void canReturnManyUniqueTimestampsInParallel(
            TimestampService timestampService,
            ExecutorService executor)
            throws InterruptedException, TimeoutException {
        Set<Long> uniqueTimestamps = new ConcurrentSkipListSet<>();

        repeat(executor, TWO_MILLION, () -> uniqueTimestamps.add(timestampService.getFreshTimestamp()));

        Assertions.assertThat(uniqueTimestamps.size()).isEqualTo((int) TWO_MILLION);
    }

    private static void repeat(ExecutorService executor, long count, Runnable task) throws InterruptedException, TimeoutException {
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
