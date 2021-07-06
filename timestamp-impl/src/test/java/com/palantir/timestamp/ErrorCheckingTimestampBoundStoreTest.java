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
package com.palantir.timestamp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.palantir.exception.PalantirInterruptedException;
import org.junit.Test;

public class ErrorCheckingTimestampBoundStoreTest {

    private final TimestampAllocationFailures allocationFailures = mock(TimestampAllocationFailures.class);
    private final TimestampBoundStore delegate = mock(TimestampBoundStore.class);
    private final ErrorCheckingTimestampBoundStore store =
            new ErrorCheckingTimestampBoundStore(delegate, allocationFailures);

    @Test
    public void shouldDelegateHandlingOfAllocationFailures() {
        RuntimeException failure = new RuntimeException();
        RuntimeException expectedException = new RuntimeException();

        doThrow(failure).when(delegate).storeUpperLimit(anyLong());
        when(allocationFailures.responseTo(failure)).thenReturn(expectedException);

        assertThatThrownBy(() -> store.storeUpperLimit(1_000)).isEqualTo(expectedException);
    }

    @Test
    public void shouldNotAllocateTimestampsIfAllocationFailuresDisallowsIt() {
        doThrow(RuntimeException.class).when(allocationFailures).verifyWeShouldIssueMoreTimestamps();

        try {
            store.storeUpperLimit(1_000);
        } catch (Exception e) {
            // ignore expected exception
        }

        verify(delegate, never()).storeUpperLimit(anyLong());
    }

    @Test
    public void shouldThrowAnInterruptedExceptionIfTheThreadIsInterrupted() {
        try {
            Thread.currentThread().interrupt();
            assertThatThrownBy(() -> store.storeUpperLimit(1_000)).isInstanceOf(PalantirInterruptedException.class);
        } finally {
            // Clear the interrupt
            assertThat(Thread.interrupted()).isTrue();
        }
    }

    @Test
    public void shouldNotTryToPersistANewLimitIfInterrupted() {
        try {
            Thread.currentThread().interrupt();
            assertThatThrownBy(() -> store.storeUpperLimit(1_000)).isInstanceOf(PalantirInterruptedException.class);
        } finally {
            // Clear the interrupt
            assertThat(Thread.interrupted()).isTrue();
        }

        verify(delegate, never()).storeUpperLimit(anyLong());
    }
}
