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

package com.palantir.atlasdb.factory.timestamp;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.junit.Test;

import com.palantir.atlasdb.config.ImmutableTimestampClientConfig;
import com.palantir.atlasdb.config.TimestampClientConfig;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampService;

public class DynamicDecoratedTimestampServiceTest {
    private final TimestampService decorated = mock(TimestampService.class);
    private final TimestampService delegate = mock(TimestampService.class);

    private final AtomicBoolean atomicBoolean = new AtomicBoolean(false);

    private final TimestampService dynamicDecorated = new DynamicDecoratedTimestampService(
            decorated,
            delegate,
            atomicBoolean::get);

    @Test
    public void dynamicallySwitchesBetweenServicesForSingleFreshTimestamp() throws Exception {
        dynamicDecorated.getFreshTimestamp();
        verify(decorated, never()).getFreshTimestamp();
        verify(delegate, times(1)).getFreshTimestamp();

        atomicBoolean.set(true);
        dynamicDecorated.getFreshTimestamp();
        verify(decorated, times(1)).getFreshTimestamp();
        verify(delegate, times(1)).getFreshTimestamp();

        atomicBoolean.set(false);
        dynamicDecorated.getFreshTimestamp();
        verify(decorated, times(1)).getFreshTimestamp();
        verify(delegate, times(2)).getFreshTimestamp();
    }

    @Test
    public void dynamicallySwitchesBetweenServicesForFreshTimestamps() throws Exception {
        dynamicDecorated.getFreshTimestamps(10);
        verify(decorated, never()).getFreshTimestamps(10);
        verify(delegate, times(1)).getFreshTimestamps(10);

        atomicBoolean.set(true);
        dynamicDecorated.getFreshTimestamps(20);
        verify(decorated, times(1)).getFreshTimestamps(20);
        verify(delegate, never()).getFreshTimestamps(20);

        atomicBoolean.set(false);
        dynamicDecorated.getFreshTimestamps(30);
        verify(decorated, never()).getFreshTimestamps(30);
        verify(delegate, times(1)).getFreshTimestamps(30);
    }

    @Test
    public void dynamicallyBatchesOnConfigChange() throws Exception {
        TimestampService inMemory = spy(InMemoryTimestampService.class);

        Supplier<TimestampClientConfig> configSupplier = () -> ImmutableTimestampClientConfig.of(atomicBoolean.get());
        TimestampService dynamicRateLimited = DynamicDecoratedTimestampService.createWithRateLimiting(
                inMemory, configSupplier);

        atomicBoolean.set(true);
        dynamicRateLimited.getFreshTimestamp();
        verify(inMemory, never()).getFreshTimestamp();

        atomicBoolean.set(false);
        dynamicRateLimited.getFreshTimestamp();
        verify(inMemory, times(1)).getFreshTimestamp();
    }
}
