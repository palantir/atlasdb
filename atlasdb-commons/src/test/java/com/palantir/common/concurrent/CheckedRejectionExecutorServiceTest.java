/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.common.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import org.junit.Test;

public class CheckedRejectionExecutorServiceTest {
    private static final Runnable DO_NOTHING = () -> {};
    private static final Callable<Integer> RETURN_ONE = () -> 1;
    private static final RejectedExecutionException REJECTED_EXECUTION_EXCEPTION = new RejectedExecutionException();

    private final ExecutorService delegate = mock(ExecutorService.class);
    private final CheckedRejectionExecutorService checkedRejectionExecutor =
            new CheckedRejectionExecutorService(delegate);

    @Test
    public void passesCallThroughExecute() throws CheckedRejectedExecutionException {
        checkedRejectionExecutor.execute(DO_NOTHING);
        verify(delegate, times(1)).execute(DO_NOTHING);
    }

    @Test
    public void passesCallThroughSubmit() throws CheckedRejectedExecutionException {
        when(delegate.submit(RETURN_ONE)).thenReturn(Futures.immediateFuture(1));
        assertThat(Futures.getUnchecked(checkedRejectionExecutor.submit(RETURN_ONE)))
                .isEqualTo(1);
        verify(delegate, times(1)).submit(RETURN_ONE);
    }

    @Test
    public void propagatesRejectionThroughExecute() {
        doThrow(REJECTED_EXECUTION_EXCEPTION).when(delegate).execute(any());
        assertThatThrownBy(() -> checkedRejectionExecutor.execute(DO_NOTHING))
                .isInstanceOf(CheckedRejectedExecutionException.class)
                .hasCause(REJECTED_EXECUTION_EXCEPTION);
    }

    @Test
    @SuppressWarnings("unchecked") // Mocks
    public void propagatesRejectionThroughSubmit() {
        doThrow(REJECTED_EXECUTION_EXCEPTION).when(delegate).submit(any(Callable.class));
        assertThatThrownBy(() -> checkedRejectionExecutor.submit(RETURN_ONE))
                .isInstanceOf(CheckedRejectedExecutionException.class)
                .hasCause(REJECTED_EXECUTION_EXCEPTION);
    }
}
