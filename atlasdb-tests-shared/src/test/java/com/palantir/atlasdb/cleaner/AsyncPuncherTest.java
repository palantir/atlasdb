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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Suppliers;
import com.palantir.common.time.Clock;
import com.palantir.common.time.SystemClock;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampService;

public class AsyncPuncherTest {

    private static final long TRANSACTION_TIMEOUT = 10;
    private static final long ASYNC_PUNCHER_INTERVAL = 1;
    private static final long MAX_INTERVALS_TO_WAIT = 100;
    private static final long PUNCHER_SEED = 100;
    private static final long PUNCHER_UPDATE_TIMESTAMP = 120;

    AsyncPuncher asyncPuncherNonSeeded;
    AsyncPuncher asyncPuncherSeeded;
    TimestampService timestampService;

    @Before
    public void setup() {
        assertTrue(PUNCHER_UPDATE_TIMESTAMP > PUNCHER_SEED);
        timestampService = new InMemoryTimestampService();
        setupNonSeeded();
        setupSeeded();
    }

    private void setupNonSeeded() {
        PuncherStore puncherStore = InMemoryPuncherStore.create();
        Clock clock = new SystemClock();
        Puncher puncher = SimplePuncher.create(puncherStore, clock, Suppliers.ofInstance(TRANSACTION_TIMEOUT));
        asyncPuncherNonSeeded = AsyncPuncher.create(puncher, ASYNC_PUNCHER_INTERVAL, Optional.empty());
    }

    private void setupSeeded() {
        PuncherStore puncherStore = InMemoryPuncherStore.create();
        Clock clock = new SystemClock();
        Puncher puncher = SimplePuncher.create(puncherStore, clock, Suppliers.ofInstance(TRANSACTION_TIMEOUT));
        asyncPuncherSeeded = AsyncPuncher.create(puncher, ASYNC_PUNCHER_INTERVAL, Optional.of(() -> PUNCHER_SEED));
    }

    @After
    public void tearDown() {
        asyncPuncherNonSeeded.shutdown();
    }

    @Test
    public void delegatesInitializationCheck() {
        Puncher delegate = mock(Puncher.class);

        // using invalid timestamp to prevent puncher from punching before the test ends, breaking the test
        // can not stub punch method as its parameter is not a reference type
        Puncher puncher = AsyncPuncher.create(delegate, ASYNC_PUNCHER_INTERVAL, Optional.empty());

        when(delegate.isInitialized())
                .thenReturn(false)
                .thenReturn(true);

        assertFalse(puncher.isInitialized());
        assertTrue(puncher.isInitialized());
    }

    @Test
    public void testPuncherDurability() throws Exception {
        long stored = timestampService.getFreshTimestamp();
        asyncPuncherNonSeeded.punch(stored);
        checkExpectedValue(asyncPuncherNonSeeded, stored);
    }


    @Test
    public void testPuncherTimestampLessThanFreshTimestamp() throws Exception {
        long stored = timestampService.getFreshTimestamp();
        asyncPuncherNonSeeded.punch(stored);
        long retrieved = Long.MIN_VALUE;
        for (int i = 0; i < MAX_INTERVALS_TO_WAIT && retrieved < stored; i++) {
            Thread.sleep(ASYNC_PUNCHER_INTERVAL);
            retrieved = asyncPuncherNonSeeded.getTimestampSupplier().get();
        }
        long freshTimestamp = timestampService.getFreshTimestamp();
        assertTrue(retrieved < freshTimestamp);
    }

    @Test
    public void testPuncherStartUpSeed() throws Exception {
        checkExpectedValue(asyncPuncherSeeded, PUNCHER_SEED);
    }

    @Test
    public void testSeededPuncherUpdate() throws Exception {
        asyncPuncherSeeded.punch(PUNCHER_UPDATE_TIMESTAMP);
        checkExpectedValue(asyncPuncherSeeded, PUNCHER_UPDATE_TIMESTAMP);
    }

    private void checkExpectedValue(Puncher puncher, long expected) throws InterruptedException {
        long retrieved = Long.MIN_VALUE;
        for (int i = 0; i < MAX_INTERVALS_TO_WAIT && retrieved < expected; i++) {
            Thread.sleep(ASYNC_PUNCHER_INTERVAL);
            retrieved = puncher.getTimestampSupplier().get();
        }
        assertEquals(expected, retrieved);
    }

}
