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
package com.palantir.atlasdb.cleaner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.base.Suppliers;
import com.palantir.common.time.Clock;
import com.palantir.common.time.SystemClock;
import com.palantir.timelock.paxos.InMemoryTimelockServices;
import com.palantir.timestamp.TimestampService;
import java.util.function.LongSupplier;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AsyncPuncherTest {

    private static final long TRANSACTION_TIMEOUT = 10;
    private static final long ASYNC_PUNCHER_INTERVAL = 1;
    private static final long MAX_INTERVALS_TO_WAIT = 100;

    private static final LongSupplier THROWING_BACKUP_TIMESTAMP_SUPPLIER = () -> {
        throw new IllegalStateException("bla");
    };

    private TimestampService timestampService;
    private InMemoryTimelockServices timelockServices;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setup() {
        timelockServices = InMemoryTimelockServices.create(tempFolder);
        timestampService = timelockServices.getTimestampService();
    }

    @After
    public void tearDown() {
        timelockServices.close();
    }

    private AsyncPuncher setupPuncher(LongSupplier backupTimestampSupplier) {
        PuncherStore puncherStore = InMemoryPuncherStore.create();
        Clock clock = new SystemClock();
        Puncher puncher = SimplePuncher.create(puncherStore, clock, Suppliers.ofInstance(TRANSACTION_TIMEOUT));
        return AsyncPuncher.create(puncher, ASYNC_PUNCHER_INTERVAL, backupTimestampSupplier);
    }

    @Test
    public void delegatesInitializationCheck() {
        Puncher delegate = mock(Puncher.class);
        Puncher puncher = AsyncPuncher.create(delegate, ASYNC_PUNCHER_INTERVAL, THROWING_BACKUP_TIMESTAMP_SUPPLIER);
        try {

            when(delegate.isInitialized()).thenReturn(false).thenReturn(true);

            assertThat(puncher.isInitialized()).isFalse();
            assertThat(puncher.isInitialized()).isTrue();
        } finally {
            puncher.shutdown();
        }
    }

    @Test
    public void testPuncherDurability() throws Exception {
        long stored = timestampService.getFreshTimestamp();
        AsyncPuncher asyncPuncher = setupPuncher(THROWING_BACKUP_TIMESTAMP_SUPPLIER);
        try {
            asyncPuncher.punch(stored);
            checkExpectedValue(asyncPuncher, stored);
        } finally {
            asyncPuncher.shutdown();
        }
    }

    @Test
    public void testPuncherTimestampLessThanFreshTimestamp() throws Exception {
        long stored = timestampService.getFreshTimestamp();
        AsyncPuncher asyncPuncher = setupPuncher(THROWING_BACKUP_TIMESTAMP_SUPPLIER);
        try {
            asyncPuncher.punch(stored);
            long retrieved = Long.MIN_VALUE;
            for (int i = 0; i < MAX_INTERVALS_TO_WAIT && retrieved < stored; i++) {
                Thread.sleep(ASYNC_PUNCHER_INTERVAL);
                retrieved = asyncPuncher.getTimestampSupplier().get();
            }
            long freshTimestamp = timestampService.getFreshTimestamp();
            assertThat(retrieved).isLessThan(freshTimestamp);
        } finally {
            asyncPuncher.shutdown();
        }
    }

    @Test
    public void punchesBackupTimestampWhenNothingWasPunched() throws Exception {
        AsyncPuncher puncher = setupPuncher(timestampService::getFreshTimestamp);
        try {
            long punchedTimestamp = timestampService.getFreshTimestamp();

            puncher.punch(punchedTimestamp);
            for (int i = 0; i < MAX_INTERVALS_TO_WAIT; i++) {
                Thread.sleep(ASYNC_PUNCHER_INTERVAL);
            }
            assertThat(puncher.getTimestampSupplier().get()).isGreaterThan(punchedTimestamp);
        } finally {
            puncher.shutdown();
        }
    }

    private void checkExpectedValue(Puncher puncher, long expected) throws InterruptedException {
        long retrieved = Long.MIN_VALUE;
        for (int i = 0; i < MAX_INTERVALS_TO_WAIT && retrieved < expected; i++) {
            Thread.sleep(ASYNC_PUNCHER_INTERVAL);
            retrieved = puncher.getTimestampSupplier().get();
        }
        assertThat(expected).isEqualTo(retrieved);
    }
}
