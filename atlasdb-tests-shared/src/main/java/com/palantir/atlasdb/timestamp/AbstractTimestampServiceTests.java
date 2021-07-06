/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.timestamp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Test;

public abstract class AbstractTimestampServiceTests {
    private static final long ONE_MILLION = 1_000_000;
    private static final long TWO_MILLION = 2 * ONE_MILLION;
    public static final int ONE_THOUSAND = 1000;

    private final TimestampService timestampService = getTimestampService();
    private final TimestampManagementService timestampManagementService = getTimestampManagementService();

    protected abstract TimestampService getTimestampService();

    protected abstract TimestampManagementService getTimestampManagementService();

    @Test
    public void timestampsAreReturnedInOrder() {
        long freshTimestamp1 = timestampService.getFreshTimestamp();
        long freshTimestamp2 = timestampService.getFreshTimestamp();

        assertThat(freshTimestamp1).isLessThan(freshTimestamp2);
    }

    @Test
    public void canRequestTimestampRangeWithGetFreshTimestamps() {
        int expectedNumTimestamps = 5;
        TimestampRange range = timestampService.getFreshTimestamps(expectedNumTimestamps);

        assertThat(range.size())
                .withFailMessage(
                        "Expected %d timestamps, got %d timestamps. (The returned range was: %d-%d)",
                        expectedNumTimestamps, range.size(), range.getLowerBound(), range.getUpperBound())
                .isGreaterThanOrEqualTo(1)
                .isLessThanOrEqualTo(expectedNumTimestamps);
    }

    @Test
    public void timestampRangesAreReturnedInNonOverlappingOrder() {
        TimestampRange timestampRange1 = timestampService.getFreshTimestamps(10);
        TimestampRange timestampRange2 = timestampService.getFreshTimestamps(10);

        long firstUpperBound = timestampRange1.getUpperBound();
        long secondLowerBound = timestampRange2.getLowerBound();

        assertThat(firstUpperBound).isLessThan(secondLowerBound);
    }

    @Test
    public void canRequestMoreTimestampsThanAreAllocatedAtOnce() {
        for (int i = 0; i < ONE_MILLION / ONE_THOUSAND; i++) {
            timestampService.getFreshTimestamps(ONE_THOUSAND);
        }

        assertThat(timestampService.getFreshTimestamp()).isGreaterThanOrEqualTo(ONE_MILLION + 1);
    }

    @Test
    public void willNotHandOutTimestampsEarlierThanAFastForward() {
        timestampManagementService.fastForwardTimestamp(TWO_MILLION);

        assertThat(timestampService.getFreshTimestamp()).isGreaterThan(TWO_MILLION);
    }

    @Test
    public void willDoNothingWhenFastForwardToEarlierTimestamp() {
        timestampManagementService.fastForwardTimestamp(TWO_MILLION);
        long ts1 = timestampService.getFreshTimestamp();
        timestampManagementService.fastForwardTimestamp(ONE_MILLION);
        long ts2 = timestampService.getFreshTimestamp();
        assertThat(ts2).isGreaterThan(TWO_MILLION);
        assertThat(ts2).isGreaterThan(ts1);
    }

    @Test
    public void canReturnManyUniqueTimestampsInParallel() throws InterruptedException, TimeoutException {
        Set<Long> uniqueTimestamps = new ConcurrentSkipListSet<>();

        repeat(ONE_MILLION, () -> uniqueTimestamps.add(timestampService.getFreshTimestamp()));

        assertThat(uniqueTimestamps).hasSize((int) ONE_MILLION);
    }

    @Test
    public void shouldThrowIfRequestingNegativeNumbersOfTimestamps() {
        assertThatThrownBy(() -> timestampService.getFreshTimestamps(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldThrowIfRequestingZeroTimestamps() {
        assertThatThrownBy(() -> timestampService.getFreshTimestamps(0)).isInstanceOf(IllegalArgumentException.class);
    }

    private static void repeat(long count, Runnable task) throws InterruptedException, TimeoutException {
        ExecutorService executor = Executors.newFixedThreadPool(16);
        for (int i = 0; i < count; i++) {
            executor.execute(task);
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);

        if (!executor.isTerminated()) {
            throw new TimeoutException("Timed out waiting for the executor to terminate");
        }
    }
}
