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
package com.palantir.atlasdb.factory.timestamp;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.palantir.exception.NotInitializedException;
import com.palantir.timestamp.TimestampService;
import org.junit.Test;

public class FreshTimestampSupplierAdapterTest {
    private final FreshTimestampSupplierAdapter adapter = new FreshTimestampSupplierAdapter();

    @Test
    public void throwsNotInitializedIfTimestampServiceNotSet() {
        assertThatThrownBy(adapter::getAsLong).isInstanceOf(NotInitializedException.class);
    }

    @Test
    public void throwsNullPointerExceptionIfSettingTimestampServiceToNull() {
        assertThatThrownBy(() -> adapter.setTimestampService(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void delegatesCallToTimestampServiceIfSet() {
        TimestampService timestampService = mock(TimestampService.class);
        adapter.setTimestampService(timestampService);
        adapter.getAsLong();
        verify(timestampService, times(1)).getFreshTimestamp();
        verifyNoMoreInteractions(timestampService);
    }

    @Test
    public void canChangeTimestampService() {
        TimestampService timestampService1 = mock(TimestampService.class);
        TimestampService timestampService2 = mock(TimestampService.class);

        adapter.setTimestampService(timestampService1);
        adapter.getAsLong();
        verify(timestampService1, times(1)).getFreshTimestamp();
        verify(timestampService2, never()).getFreshTimestamp();

        adapter.setTimestampService(timestampService2);
        adapter.getAsLong();
        verify(timestampService1, times(1)).getFreshTimestamp();
        verify(timestampService2, times(1)).getFreshTimestamp();

        verifyNoMoreInteractions(timestampService1, timestampService2);
    }

    @Test
    public void throwsNullPointerExceptionIfResettingTimestampServiceToNull() {
        TimestampService timestampService = mock(TimestampService.class);
        adapter.setTimestampService(timestampService);
        assertThatThrownBy(() -> adapter.setTimestampService(null)).isInstanceOf(NullPointerException.class);
        verifyNoMoreInteractions(timestampService);
    }

    @Test
    public void propagatesExceptionsThrownByTimestampService() {
        TimestampService timestampService = mock(TimestampService.class);
        when(timestampService.getFreshTimestamp()).thenThrow(new IllegalStateException());

        adapter.setTimestampService(timestampService);
        assertThatThrownBy(adapter::getAsLong).isInstanceOf(IllegalStateException.class);
    }
}
