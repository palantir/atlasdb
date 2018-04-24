/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.palantir.exception.PalantirInterruptedException;

public class ErrorCheckingTimestampBoundStoreTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();
    private final TimestampAllocationFailures allocationFailures = mock(TimestampAllocationFailures.class);
    private final TimestampBoundStore delegate = mock(TimestampBoundStore.class);
    private final ErrorCheckingTimestampBoundStore store = new ErrorCheckingTimestampBoundStore(delegate,
            allocationFailures);

    @Test
    public void shouldDelegateHandlingOfAllocationFailures() {
        RuntimeException failure = new RuntimeException();
        RuntimeException expectedException = new RuntimeException();

        doThrow(failure).when(delegate).storeUpperLimit(anyLong());
        when(allocationFailures.responseTo(failure)).thenReturn(expectedException);

        exception.expect(is(expectedException));

        store.storeUpperLimit(1_000);
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
            exception.expect(PalantirInterruptedException.class);

            Thread.currentThread().interrupt();

            store.storeUpperLimit(1_000);
        } finally {
            // Clear the interrupt
            Thread.interrupted();
        }
    }

    @Test
    public void shouldNotTryToPersistANewLimitIfInterrupted() {
        try {
            Thread.currentThread().interrupt();
            store.storeUpperLimit(1_000);
        } catch (Exception e) {
            // Ingnore expected exception
        } finally {
            // Clear the interrupt
            Thread.interrupted();
        }

        verify(delegate, never()).storeUpperLimit(anyLong());
    }

}
