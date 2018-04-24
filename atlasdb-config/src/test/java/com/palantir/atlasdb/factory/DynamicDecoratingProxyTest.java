/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.factory;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.palantir.timestamp.TimestampService;

public class DynamicDecoratingProxyTest {
    private final TimestampService decorated = mock(TimestampService.class);
    private final TimestampService delegate = mock(TimestampService.class);

    private final AtomicBoolean atomicBoolean = new AtomicBoolean(false);

    private final TimestampService proxy = DynamicDecoratingProxy.newProxyInstance(
            decorated,
            delegate,
            atomicBoolean::get,
            TimestampService.class);

    @Test
    public void dynamicallySwitchesCorrectlyForNoArgMethods() throws Exception {
        proxy.getFreshTimestamp();
        verify(decorated, never()).getFreshTimestamp();
        verify(delegate, times(1)).getFreshTimestamp();

        atomicBoolean.set(true);
        proxy.getFreshTimestamp();
        verify(decorated, times(1)).getFreshTimestamp();
        verify(delegate, times(1)).getFreshTimestamp();

        atomicBoolean.set(false);
        proxy.getFreshTimestamp();
        verify(decorated, times(1)).getFreshTimestamp();
        verify(delegate, times(2)).getFreshTimestamp();
    }

    @Test
    public void dynamicallySwitchesCorrectlyForMethodsWithArguments() throws Exception {
        proxy.getFreshTimestamps(10);
        verify(decorated, never()).getFreshTimestamps(10);
        verify(delegate, times(1)).getFreshTimestamps(10);

        atomicBoolean.set(true);
        proxy.getFreshTimestamps(20);
        verify(decorated, times(1)).getFreshTimestamps(20);
        verify(delegate, never()).getFreshTimestamps(20);

        atomicBoolean.set(false);
        proxy.getFreshTimestamps(30);
        verify(decorated, never()).getFreshTimestamps(30);
        verify(delegate, times(1)).getFreshTimestamps(30);
    }
}
