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

package com.palantir.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.logsafe.exceptions.SafeRuntimeException;

public class SafeShutdownRunnerTest {
    private static final RuntimeException EXCEPTION = new RuntimeException("test");

    private Runnable mockRunnable = mock(Runnable.class);
    private Runnable throwingRunnable = mock(Runnable.class);
    private Runnable verySlowUninterruptibleRunnable = mock(Runnable.class);

    @Before
    public void setupMocks() {
        doAnswer(invocation -> {
            Uninterruptibles.sleepUninterruptibly(10_000, TimeUnit.MILLISECONDS);
            return null;
        }).when(verySlowUninterruptibleRunnable).run();
        doThrow(EXCEPTION).when(throwingRunnable).run();
    }

    @Test
    public void runnerRunsOneTask() {
        SafeShutdownRunner runner = new SafeShutdownRunner(Duration.ofSeconds(1));

        runner.shutdownSafely(mockRunnable);

        verify(mockRunnable, times(1)).run();
        assertThatCode(runner::close).doesNotThrowAnyException();
    }

    @Test
    public void exceptionsAreSuppressedAndReportedWhenClosing() {
        SafeShutdownRunner runner = new SafeShutdownRunner(Duration.ofSeconds(1));

        assertThatCode(() -> runner.shutdownSafely(throwingRunnable)).doesNotThrowAnyException();
        assertThatThrownBy(runner::close)
                .isInstanceOf(SafeRuntimeException.class)
                .hasSuppressedException(EXCEPTION);
    }

    @Test
    public void slowTasksTimeOutWithoutThrowing() {
        SafeShutdownRunner runner = new SafeShutdownRunner(Duration.ofMillis(50));

        runner.shutdownSafely(verySlowUninterruptibleRunnable);
        runner.shutdownSafely(verySlowUninterruptibleRunnable);

        verify(verySlowUninterruptibleRunnable, times(2)).run();

        closeAndAssertNumberOfTimeouts(runner, 2);
    }

    @Test
    public void otherTasksStillRunInPresenceOfSlowTasksThatTimeOut() {
        SafeShutdownRunner runner = new SafeShutdownRunner(Duration.ofMillis(50));

        runner.shutdownSafely(verySlowUninterruptibleRunnable);
        runner.shutdownSafely(verySlowUninterruptibleRunnable);
        runner.shutdownSafely(mockRunnable);
        runner.shutdownSafely(verySlowUninterruptibleRunnable);
        runner.shutdownSafely(verySlowUninterruptibleRunnable);
        runner.shutdownSafely(mockRunnable);

        verify(verySlowUninterruptibleRunnable, times(4)).run();
        verify(mockRunnable, times(2)).run();

        closeAndAssertNumberOfTimeouts(runner, 4);
    }

    private void closeAndAssertNumberOfTimeouts(SafeShutdownRunner runner, int number) {
        assertThatThrownBy(runner::close)
                .isInstanceOf(RuntimeException.class)
                .satisfies(exception -> {
                    Throwable[] suppressed = exception.getSuppressed();
                    assertThat(suppressed.length).isEqualTo(number);
                    Stream.of(suppressed).forEach(th -> assertThat(th).isInstanceOf(TimeoutException.class));
                });
    }
}
