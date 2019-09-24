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

import org.junit.Test;

public class PredicateSwitchedProxyTest {
    private final TestInterface decorated = mock(TestInterface.class);
    private final TestInterface delegate = mock(TestInterface.class);

    private final AtomicBoolean atomicBoolean = new AtomicBoolean(false);

    private final TestInterface testInterface = PredicateSwitchedProxy.newProxyInstance(
            decorated,
            delegate,
            atomicBoolean::get,
            TestInterface.class);

    @Test
    public void dynamicallySwitchesCorrectlyForNoArgMethods() throws Exception {
        testInterface.noArgumentMethod();
        verify(decorated, never()).noArgumentMethod();
        verify(delegate, times(1)).noArgumentMethod();

        atomicBoolean.set(true);
        testInterface.noArgumentMethod();
        verify(decorated, times(1)).noArgumentMethod();
        verify(delegate, times(1)).noArgumentMethod();

        atomicBoolean.set(false);
        testInterface.noArgumentMethod();
        verify(decorated, times(1)).noArgumentMethod();
        verify(delegate, times(2)).noArgumentMethod();
    }

    @Test
    public void dynamicallySwitchesCorrectlyForMethodsWithArguments() throws Exception {
        testInterface.methodWithArgument(10);
        verify(decorated, never()).methodWithArgument(10);
        verify(delegate, times(1)).methodWithArgument(10);

        atomicBoolean.set(true);
        testInterface.methodWithArgument(20);
        verify(decorated, times(1)).methodWithArgument(20);
        verify(delegate, never()).methodWithArgument(20);

        atomicBoolean.set(false);
        testInterface.methodWithArgument(30);
        verify(decorated, never()).methodWithArgument(30);
        verify(delegate, times(1)).methodWithArgument(30);
    }

    interface TestInterface {
        void noArgumentMethod();

        void methodWithArgument(int argument);
    }
}
