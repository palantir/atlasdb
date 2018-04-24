/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Test;

import com.palantir.remoting3.ext.refresh.Refreshable;

public class PollingRefreshableTest {
    private static final long FORTY_TWO = 42L;
    private static final Duration REFRESH_INTERVAL = Duration.of(5, ChronoUnit.HALF_DAYS);

    private final DeterministicScheduler scheduler = new DeterministicScheduler();

    @Test
    public void refreshableIsInitializedWithTheSupplierValue() {
        PollingRefreshable<Long> pollingRefreshable = createPollingRefreshableWithTestScheduler(() -> 1L);
        assertRefreshableContainsAndClear(pollingRefreshable.getRefreshable(), 1L);
    }

    @Test
    public void refreshableIsNotRepopulatedWithStaleSupplierValuesEvenAfterTheRefreshInterval() {
        PollingRefreshable<Long> pollingRefreshable = createPollingRefreshableWithTestScheduler(() -> 1L);
        assertRefreshableContainsAndClear(pollingRefreshable.getRefreshable(), 1L);
        scheduler.tick(REFRESH_INTERVAL.toMillis() + 1, TimeUnit.MILLISECONDS);
        assertThat(pollingRefreshable.getRefreshable().getAndClear()).isNotPresent();
    }

    @Test
    public void refreshableIsRepopulatedWithNewSupplierValuesAfterTheRefreshIntervalPasses() {
        AtomicLong atomicLong = new AtomicLong();
        PollingRefreshable<Long> pollingRefreshable = createPollingRefreshableWithTestScheduler(
                atomicLong::incrementAndGet);
        Refreshable<Long> refreshable = pollingRefreshable.getRefreshable();

        assertRefreshableContainsAndClear(refreshable, 1L);
        scheduler.tick(REFRESH_INTERVAL.toMillis() + 1, TimeUnit.MILLISECONDS);
        assertRefreshableContainsAndClear(refreshable, 2L);
        scheduler.tick(REFRESH_INTERVAL.toMillis() + 1, TimeUnit.MILLISECONDS);
        assertRefreshableContainsAndClear(refreshable, 3L);
    }

    @Test
    public void refreshableIsPopulatedWithTheFreshestValueAfterTheRefreshIntervalPasses() {
        AtomicLong atomicLong = new AtomicLong();
        PollingRefreshable<Long> pollingRefreshable = createPollingRefreshableWithTestScheduler(atomicLong::get);
        Refreshable<Long> refreshable = pollingRefreshable.getRefreshable();

        assertRefreshableContainsAndClear(refreshable, 0L);
        atomicLong.set(FORTY_TWO + FORTY_TWO);
        scheduler.tick(1, TimeUnit.MILLISECONDS);
        atomicLong.set(FORTY_TWO);
        scheduler.tick(REFRESH_INTERVAL.toMillis() + 1, TimeUnit.MILLISECONDS);
        assertRefreshableContainsAndClear(refreshable, FORTY_TWO);
    }

    @Test
    public void refreshableIsNotRepopulatedWithNewSupplierValuesBeforeTheRefreshIntervalPasses() {
        AtomicLong atomicLong = new AtomicLong();
        PollingRefreshable<Long> pollingRefreshable = createPollingRefreshableWithTestScheduler(
                atomicLong::incrementAndGet);
        Refreshable<Long> refreshable = pollingRefreshable.getRefreshable();

        assertRefreshableContainsAndClear(refreshable, 1L);
        scheduler.tick(REFRESH_INTERVAL.toMillis() - 1, TimeUnit.MILLISECONDS);
        assertThat(refreshable.getAndClear()).isEmpty();
    }

    @Test
    public void canRecoverFromSupplierThrowingExceptionsInitially() {
        Refreshable<Long> refreshable = getIncrementingLongRefreshableThrowingOnSpecificValue(1L);

        assertThat(refreshable.getAndClear()).isEmpty();
        scheduler.tick(REFRESH_INTERVAL.toMillis() + 1, TimeUnit.MILLISECONDS);
        assertRefreshableContainsAndClear(refreshable, 2L);
    }

    @Test
    public void canRecoverFromSupplierThrowingExceptionsLater() {
        Refreshable<Long> refreshable = getIncrementingLongRefreshableThrowingOnSpecificValue(2L);

        assertRefreshableContainsAndClear(refreshable, 1L);

        // This execution will throw a RuntimeException.
        scheduler.tick(REFRESH_INTERVAL.toMillis() + 1, TimeUnit.MILLISECONDS);
        assertThat(refreshable.getAndClear()).isEmpty();

        scheduler.tick(REFRESH_INTERVAL.toMillis() + 1, TimeUnit.MILLISECONDS);
        assertRefreshableContainsAndClear(refreshable, 3L);
    }

    @Test
    public void shutsDownExecutorWhenClosed() {
        ScheduledExecutorService scheduledExecutor = mock(ScheduledExecutorService.class);
        PollingRefreshable<Long> pollingRefreshable = PollingRefreshable.createWithSpecificPoller(
                () -> 1L, REFRESH_INTERVAL, scheduledExecutor);
        pollingRefreshable.close();
        verify(scheduledExecutor).shutdown();
    }

    @Test
    public void canCloseMultipleTimes() {
        PollingRefreshable<Long> pollingRefreshable = PollingRefreshable.create(() -> 1L);
        pollingRefreshable.close();
        pollingRefreshable.close();
    }

    @Test
    public void canConfigureRefreshInterval() {
        Duration doubleRefreshInterval = REFRESH_INTERVAL.multipliedBy(2L);
        AtomicLong atomicLong = new AtomicLong();
        PollingRefreshable<Long> lessFrequentlyPollingRefreshable = PollingRefreshable.createWithSpecificPoller(
                atomicLong::incrementAndGet, doubleRefreshInterval, scheduler);
        Refreshable<Long> refreshable = lessFrequentlyPollingRefreshable.getRefreshable();

        assertRefreshableContainsAndClear(refreshable, 1L);

        // This is only about half of our poller's refresh interval, so we shouldn't have polled yet
        scheduler.tick(REFRESH_INTERVAL.toMillis() + 1, TimeUnit.MILLISECONDS);
        assertThat(refreshable.getAndClear()).isEmpty();

        // But now we cumulatively have ticked by 2*REFRESH_INTERVAL + 1, so we should have polled
        scheduler.tick(REFRESH_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
        assertRefreshableContainsAndClear(refreshable, 2L);
    }

    @Test
    public void cannotSetRefreshIntervalToZero() {
        assertThatThrownBy(() -> PollingRefreshable.create(() -> 1L, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void cannotSetNegativeRefreshInterval() {
        assertThatThrownBy(() -> PollingRefreshable.create(() -> 1L, Duration.of(-1, ChronoUnit.SECONDS)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private <T> PollingRefreshable<T> createPollingRefreshableWithTestScheduler(Supplier<T> supplier) {
        return PollingRefreshable.createWithSpecificPoller(supplier, REFRESH_INTERVAL, scheduler);
    }

    private <T> void assertRefreshableContainsAndClear(Refreshable<T> refreshable, T expectedValue) {
        assertThat(refreshable.getAndClear()).isPresent().contains(expectedValue);
    }

    private Refreshable<Long> getIncrementingLongRefreshableThrowingOnSpecificValue(long badValue) {
        AtomicLong atomicLong = new AtomicLong();
        PollingRefreshable<Long> pollingRefreshable = PollingRefreshable.createWithSpecificPoller(
                () -> {
                    long newValue = atomicLong.incrementAndGet();
                    if (newValue == badValue) {
                        throw new RuntimeException(badValue + " is illegal");
                    }
                    return newValue;
                },
                REFRESH_INTERVAL,
                scheduler);
        return pollingRefreshable.getRefreshable();
    }
}
