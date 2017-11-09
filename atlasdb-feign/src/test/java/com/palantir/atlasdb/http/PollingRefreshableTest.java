/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Test;

import com.palantir.remoting3.ext.refresh.Refreshable;

public class PollingRefreshableTest {
    private static final long FORTY_TWO = 42L;

    private final DeterministicScheduler scheduler = new DeterministicScheduler();

    @Test
    public void refreshableIsInitializedWithTheSupplierValue() {
        PollingRefreshable<Long> pollingRefreshable = PollingRefreshable.createWithSpecificPoller(() -> 1L, scheduler);
        assertThat(pollingRefreshable.getRefreshable().getAndClear()).isPresent().contains(1L);
    }

    @Test
    public void refreshableIsNotRepopulatedWithStaleSupplierValuesEvenAfterTheRefreshInterval() {
        PollingRefreshable<Long> pollingRefreshable = PollingRefreshable.createWithSpecificPoller(() -> 1L, scheduler);
        assertThat(pollingRefreshable.getRefreshable().getAndClear()).isPresent().contains(1L);
        scheduler.tick(PollingRefreshable.POLL_INTERVAL.toMillis() + 1, TimeUnit.MILLISECONDS);
        assertThat(pollingRefreshable.getRefreshable().getAndClear()).isNotPresent();
    }

    @Test
    public void refreshableIsRepopulatedWithNewSupplierValuesAfterTheRefreshIntervalPasses() {
        AtomicLong atomicLong = new AtomicLong();
        PollingRefreshable<Long> pollingRefreshable = PollingRefreshable.createWithSpecificPoller(
                atomicLong::incrementAndGet, scheduler);
        Refreshable<Long> refreshable = pollingRefreshable.getRefreshable();

        assertThat(refreshable.getAndClear()).isPresent().contains(1L);
        scheduler.tick(PollingRefreshable.POLL_INTERVAL.toMillis() + 1, TimeUnit.MILLISECONDS);
        assertThat(refreshable.getAndClear()).isPresent().contains(2L);
        scheduler.tick(PollingRefreshable.POLL_INTERVAL.toMillis() + 1, TimeUnit.MILLISECONDS);
        assertThat(refreshable.getAndClear()).isPresent().contains(3L);
    }

    @Test
    public void refreshableIsPopulatedWithTheFreshestValueAfterTheRefreshIntervalPasses() {
        AtomicLong atomicLong = new AtomicLong();
        PollingRefreshable<Long> pollingRefreshable = PollingRefreshable.createWithSpecificPoller(
                atomicLong::get, scheduler);
        Refreshable<Long> refreshable = pollingRefreshable.getRefreshable();

        assertThat(refreshable.getAndClear()).isPresent().contains(0L);
        atomicLong.set(FORTY_TWO + FORTY_TWO);
        scheduler.tick(1, TimeUnit.MILLISECONDS);
        atomicLong.set(FORTY_TWO);
        scheduler.tick(PollingRefreshable.POLL_INTERVAL.toMillis() + 1, TimeUnit.MILLISECONDS);
        assertThat(refreshable.getAndClear()).isPresent().contains(FORTY_TWO);
    }

    @Test
    public void refreshableIsNotRepopulatedWithNewSupplierValuesBeforeTheRefreshIntervalPasses() {
        AtomicLong atomicLong = new AtomicLong();
        PollingRefreshable<Long> pollingRefreshable = PollingRefreshable.createWithSpecificPoller(
                atomicLong::incrementAndGet, scheduler);
        Refreshable<Long> refreshable = pollingRefreshable.getRefreshable();

        assertThat(refreshable.getAndClear()).isPresent().contains(1L);
        scheduler.tick(PollingRefreshable.POLL_INTERVAL.toMillis() - 1, TimeUnit.MILLISECONDS);
        assertThat(refreshable.getAndClear()).isEmpty();
    }

    @Test
    public void shutsDownExecutorWhenClosed() {
        ScheduledExecutorService scheduledExecutor = mock(ScheduledExecutorService.class);
        PollingRefreshable<Long> pollingRefreshable = PollingRefreshable.createWithSpecificPoller(
                () -> 1L, scheduledExecutor);
        pollingRefreshable.close();
        verify(scheduledExecutor).shutdown();
    }

    @Test
    public void canCloseMultipleTimes() {
        PollingRefreshable<Long> pollingRefreshable = PollingRefreshable.create(() -> 1L);
        pollingRefreshable.close();
        pollingRefreshable.close();
    }
}
