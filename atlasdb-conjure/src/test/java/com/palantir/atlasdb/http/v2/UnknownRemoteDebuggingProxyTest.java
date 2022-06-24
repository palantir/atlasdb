/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.http.v2;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.palantir.conjure.java.api.errors.UnknownRemoteException;
import com.palantir.logsafe.SafeArg;
import com.palantir.refreshable.Refreshable;
import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BinaryOperator;
import org.junit.Before;
import org.junit.Test;

public class UnknownRemoteDebuggingProxyTest {
    private final BinaryOperator<Integer> binaryOperator = mock(BinaryOperator.class);
    private final Clock clock = mock(Clock.class);
    private final AtomicLong time = new AtomicLong();
    private BinaryOperator<Integer> debuggingProxy;

    @Before
    public void setUp() {
        setUpClock();
        createProxy();
    }

    @Test
    public void unwrapsInvocationExceptionIfPossibleAndThrows() {
        UnknownRemoteException ex = new UnknownRemoteException(308, "body");
        when(binaryOperator.apply(1, 2)).thenThrow(ex);

        assertThatThrownBy(() -> debuggingProxy.apply(1, 2)).isEqualTo(ex);
        verify(binaryOperator, times(10)).apply(1, 2);
    }

    private void setUpClock() {
        when(clock.instant()).thenAnswer($ -> Instant.ofEpochMilli(time.getAndAdd(1_000)));
    }

    private void createProxy() {
        debuggingProxy = UnknownRemoteDebuggingProxy.newProxyInstance(
                BinaryOperator.class,
                Refreshable.only(SafeArg.of("i", 1)),
                FastFailoverProxy.newProxyInstance(BinaryOperator.class, () -> binaryOperator, clock));
    }
}
