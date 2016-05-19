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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

public class PersistentUpperLimitTest {
    private static final long TIMESTAMP = 12345L;
    private final TimestampBoundStore boundStore = mock(TimestampBoundStore.class);
    private final PersistentUpperLimit upperLimit = new PersistentUpperLimit(boundStore);

    @Test
    public void shouldPersistANewUpperLimit() {
        upperLimit.store(TIMESTAMP);

        verify(boundStore).storeUpperLimit(TIMESTAMP);
    }

    @Test
    public void shouldStartWithTheCurrentStoredLimit() {
        when(boundStore.getUpperLimit()).thenReturn(TIMESTAMP);

        PersistentUpperLimit brandNewUpperLimit = new PersistentUpperLimit(boundStore);

        assertThat(brandNewUpperLimit.get(), is(TIMESTAMP));
    }

    @Test
    public void shouldReturnTheNewUpperLimitAfterItHasBeenStored() {
        upperLimit.store(TIMESTAMP);

        assertThat(upperLimit.get(), is(TIMESTAMP));
    }

    @Test
    public void shouldOnlyMakeOneGetCallToTheUnderlyingStore() {
        upperLimit.get();
        upperLimit.get();

        verify(boundStore, times(1)).getUpperLimit();
    }

    @Test
    public void shouldIncreaseTheUpperLimitIfTheNewLimitIsBigger() {
        upperLimit.store(TIMESTAMP);

        upperLimit.increaseToAtLeast(TIMESTAMP + 1000);
        assertThat(upperLimit.get(), is(TIMESTAMP + 1000));
    }

    @Test
    public void shouldNotIncreaseTheUpperLimitIfTheNewLimitIsSmaller() {
        upperLimit.store(TIMESTAMP);

        upperLimit.increaseToAtLeast(TIMESTAMP - 1000);
        assertThat(upperLimit.get(), is(TIMESTAMP));
    }

    @Test
    public void shouldPersistAnIncreasedTimestamp() {
        upperLimit.store(TIMESTAMP);

        upperLimit.increaseToAtLeast(TIMESTAMP + 1000);
        verify(boundStore).storeUpperLimit(TIMESTAMP + 1000);
    }
}
