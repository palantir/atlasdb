/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.timestamp;

import static java.util.concurrent.TimeUnit.MINUTES;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.common.time.Clock;
import com.palantir.exception.PalantirInterruptedException;

public class PersistentUpperLimitTest {
    private static final long TIMESTAMP = 12345L;
    private static final long INITIAL_UPPER_LIMIT = 10L;

    private final Clock clock = mock(Clock.class);
    private TimestampBoundStore boundStore;
    private PersistentUpperLimit upperLimit;

    @Rule
    public ExpectedException exception = ExpectedException.none();
    private final TimestampAllocationFailures allocationFailures = mock(TimestampAllocationFailures.class);

    @Before
    public void setup() {
        boundStore = mock(TimestampBoundStore.class);
        when(boundStore.getUpperLimit()).thenReturn(INITIAL_UPPER_LIMIT);
        upperLimit = new PersistentUpperLimit(boundStore, clock, allocationFailures);
    }

    @Test
    public void shouldStartWithTheCurrentStoredLimit() {
        when(boundStore.getUpperLimit()).thenReturn(TIMESTAMP);

        PersistentUpperLimit brandNewUpperLimit = new PersistentUpperLimit(boundStore, clock, new TimestampAllocationFailures());

        assertThat(brandNewUpperLimit.get(), is(TIMESTAMP));
    }

    @Test
    public void shouldOnlyMakeOneGetCallToTheUnderlyingStore() {
        upperLimit.get();
        upperLimit.get();

        verify(boundStore, times(1)).getUpperLimit();
    }

    @Test
    public void shouldIncreaseTheUpperLimitIfTheNewLimitIsBigger() {
        upperLimit.increaseToAtLeast(TIMESTAMP, 0);

        upperLimit.increaseToAtLeast(TIMESTAMP + 1000, 0);
        assertThat(upperLimit.get(), is(TIMESTAMP + 1000));
    }

    @Test
    public void shouldIncreaseTheUpperLimitWithBufferIfTheNewLimitIsBigger() {
        upperLimit.increaseToAtLeast(TIMESTAMP, 0);

        upperLimit.increaseToAtLeast(TIMESTAMP + 1000, 1000);
        assertThat(upperLimit.get(), is(TIMESTAMP + 2000));
    }

    @Test
    public void shouldNotIncreaseTheUpperLimitWithBufferIfTheNewLimitIsEqual() {
        upperLimit.increaseToAtLeast(TIMESTAMP, 0);

        upperLimit.increaseToAtLeast(TIMESTAMP, 1000);
        assertThat(upperLimit.get(), is(TIMESTAMP));
    }

    @Test
    public void shouldNotIncreaseTheUpperLimitIfTheNewLimitIsSmaller() {
        upperLimit.increaseToAtLeast(TIMESTAMP, 0);

        upperLimit.increaseToAtLeast(TIMESTAMP - 1000, 0);
        assertThat(upperLimit.get(), is(TIMESTAMP));
    }

    @Test
    public void shouldNotIncreaseTheUpperLimitIfTheNewLimitIsSmallerRegardlessOfBuffer() {
        upperLimit.increaseToAtLeast(TIMESTAMP, 0);

        upperLimit.increaseToAtLeast(TIMESTAMP - 1000, 2000);
        assertThat(upperLimit.get(), is(TIMESTAMP));
    }

    @Test
    public void shouldPersistAnIncreasedTimestamp() {
        upperLimit.increaseToAtLeast(TIMESTAMP, 0);

        upperLimit.increaseToAtLeast(TIMESTAMP + 1000, 1000);
        verify(boundStore).storeUpperLimit(TIMESTAMP + 2000);
    }

    @Test
    public void shouldNotChangeTheCurrentUpperLimitIfItFailsToPersist() {
        doThrow(RuntimeException.class).when(boundStore).storeUpperLimit(anyLong());

        try {
            upperLimit.increaseToAtLeast(INITIAL_UPPER_LIMIT + 10, 0);
        } catch (Exception e) {
            // We expect this to throw
        }

        assertThat(upperLimit.get(), is(INITIAL_UPPER_LIMIT));
    }

    @Test
    public void shouldKnowIfItWasUpdateWithinACertainTimeframe() {
        whenTheTimeIs(1, MINUTES);

        upperLimit.increaseToAtLeast(TIMESTAMP, 0);

        whenTheTimeIs(4, MINUTES);

        assertThat(upperLimit.hasIncreasedWithin(2, MINUTES), is(false));
    }

    @Test
    public void shouldKnowIfItWasNotUpdateWithinACertainTimeframe() {
        whenTheTimeIs(1, MINUTES);

        upperLimit.increaseToAtLeast(TIMESTAMP, 0);

        whenTheTimeIs(2, MINUTES);

        assertThat(upperLimit.hasIncreasedWithin(2, MINUTES), is(true));
    }

    @Test
    public void shouldDelegateHandlingOfAllocationFailures() {
        RuntimeException failure = new RuntimeException();
        RuntimeException expectedException = new RuntimeException();

        doThrow(failure).when(boundStore).storeUpperLimit(anyLong());
        when(allocationFailures.responseTo(failure)).thenReturn(expectedException);

        exception.expect(is(expectedException));

        upperLimit.increaseToAtLeast(INITIAL_UPPER_LIMIT + 10, 0);
    }

    @Test
    public void shouldNotAllocateTimestampsIfAllocationFailuresDisallowsIt() {
        doThrow(RuntimeException.class).when(allocationFailures).verifyWeShouldIssueMoreTimestamps();

        try {
            upperLimit.increaseToAtLeast(INITIAL_UPPER_LIMIT + 10, 0);
        } catch (Exception e) {
            // ignore expected exception
        }

        verify(boundStore, never()).storeUpperLimit(anyLong());
    }

    @Test
    public void shouldNotIssueTimestampsIfAllocationFailuresDisallowsIt() {
        doThrow(ServiceNotAvailableException.class).when(allocationFailures).verifyWeShouldIssueMoreTimestamps();

        exception.expect(ServiceNotAvailableException.class);

        upperLimit.get();
    }

    @Test
    public void shouldThrowAnInterruptedExceptionIfTheThreadIsInterrupted() {
        try {
            exception.expect(PalantirInterruptedException.class);

            Thread.currentThread().interrupt();

            upperLimit.increaseToAtLeast(INITIAL_UPPER_LIMIT + 10, 0);
        } finally {
            // Clear the interrupt
            Thread.interrupted();
        }
    }

    @Test
    public void shouldNotTryToPersistANewLimitIfInterrupted() {
        try {
            Thread.currentThread().interrupt();
            upperLimit.increaseToAtLeast(INITIAL_UPPER_LIMIT + 10, 0);
        } catch (Exception e) {
            // Ingnore expected exception
        } finally {
            // Clear the interrupt
            Thread.interrupted();
        }

        verify(boundStore, never()).storeUpperLimit(anyLong());
    }

    private void whenTheTimeIs(long time, TimeUnit unit) {
        when(clock.getTimeMillis()).thenReturn(unit.toMillis(time));
    }


}
