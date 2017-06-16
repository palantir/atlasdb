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

package com.palantir.atlasdb.cleaner;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.google.common.base.Supplier;

public class ImmutableTimestampProgressMonitorRunnerTest {
    @Test
    public void testProgressFailure() throws Exception {
        Runnable mockRunnableIfFailed = mock(Runnable.class);
        Runnable runnable = ImmutableTimestampProgressMonitorRunner.of(() -> 0L, mockRunnableIfFailed);
        runnable.run();
        verify(mockRunnableIfFailed, times(1)).run();
    }

    @Test
    public void testProgressOk() throws Exception {
        Runnable mockRunnableIfFailed = mock(Runnable.class);
        Supplier mockedSupplier = mock(Supplier.class);
        when(mockedSupplier.get()).thenReturn(0L).thenReturn(1L);
        Runnable runnable = ImmutableTimestampProgressMonitorRunner.of(mockedSupplier, mockRunnableIfFailed);
        runnable.run();
        verify(mockRunnableIfFailed, times(0)).run();
    }
}