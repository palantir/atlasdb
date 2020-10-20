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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ReplaceIfExceptionMatchingProxyTest {
    @Mock
    private TestInterface delegate;

    @Mock
    private Supplier<TestInterface> supplier;

    interface TestInterface {
        void doSomething();
    }

    @Before
    public void before() {
        when(supplier.get()).thenReturn(delegate);
    }

    @Test
    public void lazilyInitialized() {
        TestInterface iface =
                ReplaceIfExceptionMatchingProxy.newProxyInstance(TestInterface.class, supplier, _thrown -> true);
        verify(supplier, never()).get();
        iface.doSomething();
        verify(supplier, times(1)).get();
    }

    @Test
    public void testExceptionMatching() {
        RuntimeException exception = new RuntimeException();
        TestInterface iface = ReplaceIfExceptionMatchingProxy.newProxyInstance(
                TestInterface.class, supplier, thrown -> thrown.equals(exception));
        iface.doSomething();
        iface.doSomething();
        iface.doSomething();
        verify(supplier, times(1)).get();
        doThrow(exception).when(delegate).doSomething();
        assertThatThrownBy(iface::doSomething).isEqualTo(exception);
        verify(supplier, times(2)).get();
    }

    @Test
    public void testEqualsHashCodeToStringNotDelegated() {
        TestInterface iface =
                ReplaceIfExceptionMatchingProxy.newProxyInstance(TestInterface.class, supplier, _thrown -> true);
        assertThat(iface.toString()).isNotEqualTo(delegate.toString());
        assertThat(iface).isEqualTo(iface);
        assertThat(iface).isNotEqualTo(delegate);
        assertThat(iface.hashCode()).isNotEqualTo(delegate.hashCode());
    }
}
