/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntConsumer;
import org.junit.Test;

public class PredicateSwitchedProxyTest {
    private final IntConsumer decorated = mock(IntConsumer.class);
    private final IntConsumer delegate = mock(IntConsumer.class);

    private final AtomicBoolean atomicBoolean = new AtomicBoolean(false);

    private final IntConsumer consumer = PredicateSwitchedProxy.newProxyInstance(
            decorated,
            delegate,
            atomicBoolean::get,
            IntConsumer.class);

    @Test
    public void dynamicallySwitchesCorrectlyForMethodsWithArguments() throws Exception {
        consumer.accept(10);
        verify(decorated, never()).accept(10);
        verify(delegate, times(1)).accept(10);

        atomicBoolean.set(true);
        consumer.accept(20);
        verify(decorated, times(1)).accept(20);
        verify(delegate, never()).accept(20);

        atomicBoolean.set(false);
        consumer.accept(30);
        verify(decorated, never()).accept(30);
        verify(delegate, times(1)).accept(30);
    }
}
