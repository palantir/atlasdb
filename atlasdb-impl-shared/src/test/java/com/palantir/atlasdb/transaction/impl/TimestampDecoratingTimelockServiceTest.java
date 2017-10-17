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

package com.palantir.atlasdb.transaction.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.palantir.lock.v2.LockImmutableTimestampRequest;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timestamp.TimestampService;

public class TimestampDecoratingTimelockServiceTest {
    private final TimelockService delegate = mock(TimelockService.class);
    private final TimestampService decoratedTimestamps = mock(TimestampService.class);
    private final TimelockService decoratingService =
            new TimestampDecoratingTimelockService(delegate, decoratedTimestamps);

    @Before
    public void setUp() {
        when(delegate.isInitialized()).thenReturn(true);
        when(decoratedTimestamps.isInitialized()).thenReturn(true);
    }

    @After
    public void verifyNoOtherCallsOnDelegates() {
        verifyNoMoreInteractions(delegate, decoratedTimestamps);
    }


    @Test
    public void isInitializedWhenPrerequisitesAreInitialized() {
        assertTrue(decoratingService.isInitialized());

        verify(delegate).isInitialized();
        verify(decoratedTimestamps).isInitialized();
    }

    @Test
    public void isNotInitializedWhenScrubberIsNotInitialized() {
        when(delegate.isInitialized()).thenReturn(false);
        assertFalse(decoratingService.isInitialized());

        verify(delegate).isInitialized();
    }

    @Test
    public void isInitializedWhenPuncherIsNotInitialized() {
        when(decoratedTimestamps.isInitialized()).thenReturn(false);
        assertFalse(decoratingService.isInitialized());

        verify(delegate).isInitialized();
        verify(decoratedTimestamps).isInitialized();
    }

    @Test
    public void singleFreshTimestampRoutedToDecoratedService() {
        decoratingService.getFreshTimestamp();
        verify(decoratedTimestamps).getFreshTimestamp();
    }

    @Test
    public void multipleFreshTimestampsRoutedToDecoratedService() {
        int numTimestamps = 42;
        decoratingService.getFreshTimestamps(numTimestamps);
        verify(decoratedTimestamps).getFreshTimestamps(numTimestamps);
    }

    @Test
    public void methodsNotOnTimestampServiceRoutedToDelegateService() {
        decoratingService.currentTimeMillis();
        verify(delegate).currentTimeMillis();

        LockImmutableTimestampRequest immutableTimestampRequest = LockImmutableTimestampRequest.create();
        decoratingService.lockImmutableTimestamp(immutableTimestampRequest);
        verify(delegate).lockImmutableTimestamp(eq(immutableTimestampRequest));
    }
}
