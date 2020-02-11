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

package com.palantir.common.proxy;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;

import com.google.common.util.concurrent.Uninterruptibles;

@SuppressWarnings("unchecked") // Mocks of generics
public class SelfRefreshingProxyTest {
    private final Supplier<Runnable> mockSupplier = mock(Supplier.class);
    private final Runnable mockRunnable = mock(Runnable.class);

    @Before
    public void setUp() {
        when(mockSupplier.get()).thenReturn(mockRunnable);
    }

    @Test
    public void doesNotRefreshProxyByDefault() {
        Runnable runnable = SelfRefreshingProxy.create(mockSupplier, Runnable.class);
        runnable.run();
        runnable.run();
        verify(mockSupplier, times(1)).get();
        verify(mockRunnable, times(2)).run();
    }

    @Test
    public void refreshesProxyAfterTimeWindow() {
        Runnable runnable = SelfRefreshingProxy.create(mockSupplier, Runnable.class, Duration.ofNanos(1));
        runnable.run();
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.NANOSECONDS);
        runnable.run();
        verify(mockSupplier, times(2)).get();
    }

    @Test
    public void propagatesExceptionsCorrectly() {
        Exception exception = new RuntimeException("bad times");
        doThrow(exception).when(mockRunnable).run();
        Runnable runnable = SelfRefreshingProxy.create(mockSupplier, Runnable.class);
        assertThatThrownBy(runnable::run).isEqualTo(exception);
    }
}
