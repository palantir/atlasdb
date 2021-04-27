/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.palantir.util.TimedRunner.TaskContext;
import java.time.Duration;
import java.util.concurrent.Callable;
import org.junit.Before;
import org.junit.Test;

public final class TimedRunnerTest {
    private static final RuntimeException EXCEPTION_1 = new RuntimeException("test");
    private static final RuntimeException EXCEPTION_2 = new RuntimeException("bleh");

    private Runnable noOpRunnable = mock(Runnable.class);
    private Runnable throwingRunnable = mock(Runnable.class);

    @Before
    public void setupMocks() {
        doThrow(EXCEPTION_1).when(throwingRunnable).run();
    }

    @Test
    public void runnerRunsOneTask() throws Exception {
        TimedRunner runner = TimedRunner.create(Duration.ofSeconds(1));

        Callable<Integer> task = mock(Callable.class);
        when(task.call()).thenReturn(5);
        int result = runner.run(TaskContext.create(task, noOpRunnable));

        assertThat(result).isEqualTo(5);
        verify(task).call();
        verify(noOpRunnable, never()).run();
    }

    @Test
    public void exceptionsAreThrownWhenRunningSingleton() {
        TimedRunner runner = TimedRunner.create(Duration.ofSeconds(1));
        Runnable failureHandler = mock(Runnable.class);

        assertThatThrownBy(() -> runner.run(TaskContext.createRunnable(throwingRunnable, failureHandler)))
                .isEqualTo(EXCEPTION_1);
        verify(failureHandler).run();
    }

    @Test
    public void correctSuppressedExceptionsAreThrownIfFailureHandlerThrows() {
        TimedRunner runner = TimedRunner.create(Duration.ofSeconds(1));
        Runnable failureHandler = mock(Runnable.class);
        doThrow(EXCEPTION_2).when(failureHandler).run();

        assertThatThrownBy(() -> runner.run(TaskContext.createRunnable(throwingRunnable, failureHandler)))
                .isEqualTo(EXCEPTION_1)
                .hasSuppressedException(EXCEPTION_2);
        verify(failureHandler).run();
    }
}
