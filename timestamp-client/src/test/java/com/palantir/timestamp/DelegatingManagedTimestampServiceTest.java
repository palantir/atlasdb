/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.timestamp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

public class DelegatingManagedTimestampServiceTest {
    private static final int FORTY_TWO = 42;
    private static final long LEADING_PI_DIGITS = 314159265358979L;

    private final TimestampService timestampService = mock(TimestampService.class);
    private final TimestampManagementService timestampManagementService = mock(TimestampManagementService.class);
    private final ManagedTimestampService managedTimestampService =
            new DelegatingManagedTimestampService(timestampService, timestampManagementService);

    @Test
    public void testIsInitializedDefersToTimestampService() {
        when(timestampService.isInitialized()).thenReturn(false).thenReturn(true);

        assertThat(managedTimestampService.isInitialized()).isFalse();
        assertThat(managedTimestampService.isInitialized()).isTrue();
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

    @Test
    public void testCannotInitialiseWithNullTimestampService() {
        assertThatThrownBy(() -> new DelegatingManagedTimestampService(timestampService, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testCannotInitialiseWithNullTimestampManagementService() {
        assertThatThrownBy(() -> new DelegatingManagedTimestampService(null, timestampManagementService))
                .isInstanceOf(NullPointerException.class);
    }
}
