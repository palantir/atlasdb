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

package com.palantir.paxos;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Before;
import org.junit.Test;

public class BatchingSupplierTest {

    private final DeterministicScheduler executor = new DeterministicScheduler();
    private final Supplier<String> delegate = mock(Supplier.class);
    private final BatchingSupplier<String> supplier = new BatchingSupplier<>(delegate, executor);

    @Before
    public void before() {
        when(delegate.get()).thenReturn("foo");
    }

    @Test
    public void delegates_to_delegate() {
        Future<String> result = supplier.get();

        executor.runNextPendingCommand();

        assertThat(getUnchecked(result)).isEqualTo("foo");
    }

    @Test
    public void batches_concurrent_requests() throws InterruptedException {
        supplier.get();
        supplier.get();
        supplier.get();

        executor.runUntilIdle();

        verify(delegate, times(1)).get();
    }

    @Test
    public void does_not_batch_serial_requests() {
        supplier.get();
        executor.runNextPendingCommand();
        supplier.get();
        executor.runNextPendingCommand();

        verify(delegate, times(2)).get();
    }

    @Test
    public void batched_requests_receive_same_result() {
        String response1 = "foo";
        String response2 = "bar";

        when(delegate.get()).thenReturn(response1);
        Collection<Future<String>> batch1 = getBatch(2);
        executor.runNextPendingCommand();

        when(delegate.get()).thenReturn(response2);
        Collection<Future<String>> batch2 = getBatch(2);
        executor.runUntilIdle();

        batch1.forEach(future -> assertThat(getUnchecked(future)).isEqualTo(response1));
        batch2.forEach(future -> assertThat(getUnchecked(future)).isEqualTo(response2));
    }

    @Test
    public void returned_future_can_complete_exceptionally() {
        RuntimeException expected = new RuntimeException("foo");

        when(delegate.get()).thenThrow(expected);

        Future<String> result = supplier.get();
        executor.runUntilIdle();

        assertThatThrownBy(() -> result.get()).hasCause(expected);
    }

    private List<Future<String>> getBatch(int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> supplier.get())
                .collect(Collectors.toList());
    }

    private <T> T getUnchecked(Future<T> future) {
        try {
            return future.get();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
