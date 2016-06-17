/**
 * Copyright 2016 Palantir Technologies
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
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

public class PersistentUpperLimitTest {
    private static final long TIMESTAMP = 12345L;
    private static final long INITIAL_UPPER_LIMIT = 10L;

    private final Clock clock = mock(Clock.class);
    private TimestampBoundStore boundStore;
    private PersistentUpperLimit upperLimit;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setup() {
        boundStore = mock(TimestampBoundStore.class);
        when(boundStore.getUpperLimit()).thenReturn(INITIAL_UPPER_LIMIT);
        upperLimit = new PersistentUpperLimit(boundStore, clock, new TimestampAllocationFailures());
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
        upperLimit.increaseToAtLeast(TIMESTAMP);

        upperLimit.increaseToAtLeast(TIMESTAMP + 1000);
        assertThat(upperLimit.get(), is(TIMESTAMP + 1000));
    }

    @Test
    public void shouldNotIncreaseTheUpperLimitIfTheNewLimitIsSmaller() {
        upperLimit.increaseToAtLeast(TIMESTAMP);

        upperLimit.increaseToAtLeast(TIMESTAMP - 1000);
        assertThat(upperLimit.get(), is(TIMESTAMP));
    }

    @Test
    public void shouldPersistAnIncreasedTimestamp() {
        upperLimit.increaseToAtLeast(TIMESTAMP);

        upperLimit.increaseToAtLeast(TIMESTAMP + 1000);
        verify(boundStore).storeUpperLimit(TIMESTAMP + 1000);
    }

    @Test
    public void shouldNotChangeTheCurrentUpperLimitIfItFailsToPersist() {
        doThrow(RuntimeException.class).when(boundStore).storeUpperLimit(anyLong());

        try {
            upperLimit.increaseToAtLeast(INITIAL_UPPER_LIMIT + 10);
        } catch (Exception e) {
            // We expect this to throw
        }

        assertThat(upperLimit.get(), is(INITIAL_UPPER_LIMIT));
    }

    @Test
    public void shouldKnowIfItWasUpdateWithinACertainTimeframe() {
        whenTheTimeIs(1, MINUTES);

        upperLimit.increaseToAtLeast(TIMESTAMP);

        whenTheTimeIs(4, MINUTES);

        assertThat(upperLimit.hasIncreasedWithin(2, MINUTES), is(false));
    }

    @Test
    public void shouldKnowIfItWasNotUpdateWithinACertainTimeframe() {
        whenTheTimeIs(1, MINUTES);

        upperLimit.increaseToAtLeast(TIMESTAMP);

        whenTheTimeIs(2, MINUTES);

        assertThat(upperLimit.hasIncreasedWithin(2, MINUTES), is(true));
    }

    @Test
    public void shouldRethrowAllocationFailures() {
        IllegalArgumentException allocationFailure = new IllegalArgumentException();

        doThrow(allocationFailure).when(boundStore).storeUpperLimit(anyLong());

        exception.expect(equalTo(allocationFailure));

        upperLimit.increaseToAtLeast(INITIAL_UPPER_LIMIT + 10);
    }

    @Test
    public void shouldMultipleServicesRunningExceptionsInServiceNotAvailableException() {
        MultipleRunningTimestampServiceError allocationFailure = new MultipleRunningTimestampServiceError("error");

        doThrow(allocationFailure).when(boundStore).storeUpperLimit(anyLong());

        exception.expectCause(equalTo(allocationFailure));
        exception.expect(ServiceNotAvailableException.class);

        upperLimit.increaseToAtLeast(INITIAL_UPPER_LIMIT + 10);
    }

    @Test
    public void shouldNotAttemptToPersistAnyMoreValuesAfterCatchingAMultipleRunningTimestampServiceError() {
        MultipleRunningTimestampServiceError allocationFailure = new MultipleRunningTimestampServiceError("error");

        doThrow(allocationFailure).when(boundStore).storeUpperLimit(anyLong());

        increaseToAtleastIgnoringErrors(INITIAL_UPPER_LIMIT + 10);
        increaseToAtleastIgnoringErrors(INITIAL_UPPER_LIMIT + 20);

        verify(boundStore, times(1)).storeUpperLimit(anyLong());
    }

    private void increaseToAtleastIgnoringErrors(long newValue) {
        try {
            upperLimit.increaseToAtLeast(newValue);
        } catch (Exception e) {
            // ignore expected errors
        }
    }

    private void whenTheTimeIs(long time, TimeUnit unit) {
        when(clock.getTimeMillis()).thenReturn(unit.toMillis(time));
    }


}
