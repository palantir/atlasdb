/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.queue;

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.common.util.concurrent.SettableFuture;
import com.palantir.exception.NotInitializedException;
import com.palantir.logsafe.SafeArg;
import org.junit.jupiter.api.Test;

public final class DelegatingMultiTableSweepQueueWriterTest {
    private final SettableFuture<MultiTableSweepQueueWriter> delegate = SettableFuture.create();
    private final DelegatingMultiTableSweepQueueWriter writer = new DelegatingMultiTableSweepQueueWriter(delegate);

    @Test
    public void methodFailsWithNotInitializedExceptionIfDelegateNotSet() {
        assertThatLoggableExceptionThrownBy(() -> writer.enqueue(null))
                .isInstanceOf(NotInitializedException.class)
                .hasExactlyArgs(SafeArg.of("objectName", DelegatingMultiTableSweepQueueWriter.class.getSimpleName()));
    }

    @Test
    public void methodFailsWithNotInitializedExceptionWithCauseIfDelegateFailed() {
        Throwable cause = new RuntimeException();
        delegate.setException(cause);
        assertThatLoggableExceptionThrownBy(() -> writer.enqueue(null))
                .isInstanceOf(NotInitializedException.class)
                .hasExactlyArgs(SafeArg.of("objectName", DelegatingMultiTableSweepQueueWriter.class.getSimpleName()))
                .hasRootCause(cause);
    }

    @Test
    public void methodDelegatesToUnderlyingOnceSet() {
        MultiTableSweepQueueWriter delegateWriter = mock(MultiTableSweepQueueWriter.class);
        delegate.set(delegateWriter);

        writer.enqueue(null, 0L);
        verify(delegateWriter).enqueue(null, 0L);
    }
}
