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
package com.palantir.atlasdb.timelock.paxos;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;

public class DelegatingManagedTimestampServiceTest {
    private static final int FORTY_TWO = 42;
    private static final long LEADING_PI_DIGITS = 314159265358979L;

    private final TimestampService timestampService = mock(TimestampService.class);
    private final TimestampManagementService timestampManagementService = mock(TimestampManagementService.class);
    private final ManagedTimestampService managedTimestampService = new DelegatingManagedTimestampService(
            timestampService,
            timestampManagementService);

    @Test
    public void testIsInitializedDefersToTimestampService() {
        when(timestampService.isInitialized())
                .thenReturn(false)
                .thenReturn(true);

        assertFalse(managedTimestampService.isInitialized());
        assertTrue(managedTimestampService.isInitialized());
    }

    @Test
    public void testGetFreshTimestampDefersToTimestampService() {
        managedTimestampService.getFreshTimestamp();

        verify(timestampService, times(1)).getFreshTimestamp();
    }

    @Test
    public void testGetFreshTimestampsDefersToTimestampService() {
        managedTimestampService.getFreshTimestamps(FORTY_TWO);

        verify(timestampService, times(1)).getFreshTimestamps(eq(FORTY_TWO));
    }

    @Test
    public void testFastForwardTimestampDefersToTimestampManagementService() {
        managedTimestampService.fastForwardTimestamp(LEADING_PI_DIGITS);

        verify(timestampManagementService, times(1)).fastForwardTimestamp(eq(LEADING_PI_DIGITS));
    }

    @Test(expected = NullPointerException.class)
    public void testCannotInitialiseWithNullTimestampService() {
        new DelegatingManagedTimestampService(timestampService, null);
    }

    @Test(expected = NullPointerException.class)
    public void testCannotInitialiseWithNullTimestampManagementService() {
        new DelegatingManagedTimestampService(null, timestampManagementService);
    }
}
