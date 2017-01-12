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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampServiceWithManagement;

public abstract class AbstractTimestampServiceTests {
    private static final long ONE_MILLION = 1000 * 1000;
    private static final long TWO_MILLION = 2 * ONE_MILLION;
    private TimestampServiceWithManagement timestampServiceWithManagement = getTimestampServiceWithManagement();

    protected abstract TimestampServiceWithManagement getTimestampServiceWithManagement();

    @Test
    public void timestampsAreReturnedInOrder() {
        List<Long> timestamps = new ArrayList<>();

        timestamps.add(timestampServiceWithManagement.getFreshTimestamp());
        timestamps.add(timestampServiceWithManagement.getFreshTimestamp());

        Assertions.assertThat(timestamps.get(0)).isLessThan(timestamps.get(1));
    }

    @Test
    public void canRequestTimestampRangeWithGetFreshTimestamps() {
        int expectedNumTimestamps = 5;
        TimestampRange timestampRange = timestampServiceWithManagement.getFreshTimestamps(expectedNumTimestamps);

        Assertions.assertThat((int) timestampRange.size())
                .withFailMessage("Expected %d timestamps, got %d timestamps. (The returned range was: %d-%d)",
                        expectedNumTimestamps, timestampRange.size(), timestampRange.getLowerBound(), timestampRange.getUpperBound())
                .isGreaterThanOrEqualTo(1)
                .isLessThanOrEqualTo(expectedNumTimestamps);
    }

    @Test
    public void timestampRangesAreReturnedInNonOverlappingOrder() {
        List<TimestampRange> timestampRanges = new ArrayList<>();

        timestampRanges.add(timestampServiceWithManagement.getFreshTimestamps(10));
        timestampRanges.add(timestampServiceWithManagement.getFreshTimestamps(10));

        long firstUpperBound = timestampRanges.get(0).getUpperBound();
        long secondLowerBound = timestampRanges.get(1).getLowerBound();

        Assertions.assertThat(firstUpperBound).isLessThan(secondLowerBound);
    }

    @Test
    public void canRequestMoreTimestampsThanAreAllocatedAtOnce() {
        for (int i = 0; i < ONE_MILLION / 1000; i++) {
            timestampServiceWithManagement.getFreshTimestamps(1000);
        }

        Assertions.assertThat(timestampServiceWithManagement.getFreshTimestamp())
                .isGreaterThanOrEqualTo(ONE_MILLION + 1);
    }

    @Test
    public void willNotHandOutTimestampsEarlierThanAFastForward() {
        timestampServiceWithManagement.fastForwardTimestamp(TWO_MILLION);

        Assertions.assertThat(timestampServiceWithManagement.getFreshTimestamp()).isGreaterThan(TWO_MILLION);
    }

    @Test
    public void willDoNothingWhenFastForwardToEarlierTimestamp() {
        timestampServiceWithManagement.fastForwardTimestamp(TWO_MILLION);
        long ts1 = timestampServiceWithManagement.getFreshTimestamp();
        timestampServiceWithManagement.fastForwardTimestamp(ONE_MILLION);
        long ts2 = timestampServiceWithManagement.getFreshTimestamp();
        Assertions.assertThat(ts2).isGreaterThan(TWO_MILLION);
        Assertions.assertThat(ts2).isGreaterThan(ts1);
    }

    @Test
    public void canReturnManyUniqueTimestampsInParallel()
            throws InterruptedException, TimeoutException {
        Set<Long> uniqueTimestamps = new ConcurrentSkipListSet<>();

        repeat(TWO_MILLION, () -> uniqueTimestamps.add(timestampServiceWithManagement.getFreshTimestamp()));

        Assertions.assertThat(uniqueTimestamps.size()).isEqualTo((int) TWO_MILLION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfRequestingNegativeNumbersOfTimestamps() {
       timestampServiceWithManagement.getFreshTimestamps(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfRequestingZeroTimestamps() {
        timestampServiceWithManagement.getFreshTimestamps(0);
    }

    private static void repeat(long count, Runnable task)
            throws InterruptedException, TimeoutException {
        ExecutorService executor = Executors.newFixedThreadPool(16);
        for (int i = 0; i < count; i++) {
            executor.submit(task);
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);

        if (!executor.isTerminated()) {
            throw new TimeoutException("Timed out waiting for the executor to terminate");
        }
    }
}
