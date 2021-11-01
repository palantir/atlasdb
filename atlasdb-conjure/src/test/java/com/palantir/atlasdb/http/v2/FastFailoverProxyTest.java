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

package com.palantir.atlasdb.http.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.palantir.conjure.java.api.errors.QosException;
import com.palantir.logsafe.exceptions.SafeIoException;
import feign.RetryableException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Date;
import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BinaryOperator;
import java.util.function.LongConsumer;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("unchecked") // Mock usage
public class FastFailoverProxyTest {
    private final BinaryOperator<Integer> binaryOperator = mock(BinaryOperator.class);
    private final Clock clock = mock(Clock.class);
    private final AtomicLong time = new AtomicLong();

    private BinaryOperator<Integer> proxy;

    @Before
    public void setUp() {
        setUpClock();
        createProxy();
    }

    @Test
    public void throwsNonRetryableExceptionsFromProxyWithoutRetrying() {
        IllegalStateException illegalStateException = new IllegalStateException();
        when(binaryOperator.apply(1, 2)).thenThrow(illegalStateException);

        assertThatThrownBy(() -> proxy.apply(1, 2)).isEqualTo(illegalStateException);
        verify(binaryOperator).apply(1, 2);
    }

    @Test
    public void throwsRetryableExceptionsNotCausedByRetryOther() {
        RetryableException retryableException = createRetryableException(QosException.throttle());
        when(binaryOperator.apply(1, 2)).thenThrow(retryableException);

        assertThatThrownBy(() -> proxy.apply(1, 2)).isEqualTo(retryableException);
        verify(binaryOperator).apply(1, 2);
    }

    @Test
    public void recoversFromRetryableExceptionsCausedByRetryOther() {
        RetryableException retryableException = createRetryableException(QosException.retryOther(createUrl()));
        when(binaryOperator.apply(1, 2))
                .thenThrow(retryableException)
                .thenThrow(retryableException)
                .thenThrow(retryableException)
                .thenThrow(retryableException)
                .thenReturn(3);

        assertThat(proxy.apply(1, 2)).isEqualTo(3);
        verify(binaryOperator, times(5)).apply(1, 2);
    }

    @Test
    public void eventuallyGivesUpFromRetryingRetryOther() {
        RetryableException retryableException = createRetryableException(QosException.retryOther(createUrl()));
        when(binaryOperator.apply(1, 2)).thenThrow(retryableException);

        assertThatThrownBy(() -> proxy.apply(1, 2)).isEqualTo(retryableException);
        verify(binaryOperator, times(10)).apply(1, 2);
    }

    @Test
    public void canInvokeVoidReturningMethodsSafely() {
        LongConsumer longConsumer = mock(LongConsumer.class);
        RetryableException retryableException = createRetryableException(QosException.retryOther(createUrl()));
        doThrow(retryableException).doNothing().when(longConsumer).accept(42L);

        LongConsumer proxyConsumer = FastFailoverProxy.newProxyInstance(LongConsumer.class, () -> longConsumer, clock);
        assertThatCode(() -> proxyConsumer.accept(42L)).doesNotThrowAnyException();
        verify(longConsumer, times(2)).accept(42L);
    }

    @Test
    public void alwaysInvokesMethodAtLeastOnce() {
        when(clock.instant()).thenAnswer(_$ -> Instant.ofEpochMilli(time.getAndAdd(1_000_000)));
        when(binaryOperator.apply(1, 2)).thenReturn(3);

        assertThat(proxy.apply(1, 2)).isEqualTo(3);
        verify(binaryOperator).apply(1, 2);
    }

    @Test
    public void simpleRetryableExceptionIsNotCausedByRetryOther() {
        RetryableException retryableException = new RetryableException("foo", Date.from(Instant.EPOCH));
        assertThat(FastFailoverProxy.isCausedByRetryOther(retryableException)).isFalse();
    }

    @Test
    public void retryableExceptionWithAlternativeCauseIsNotCausedByRetryOther() {
        RetryableException retryableException = createRetryableException(QosException.throttle());
        assertThat(FastFailoverProxy.isCausedByRetryOther(retryableException)).isFalse();
    }

    @Test
    public void retryableExceptionWithRetryOtherAsDirectCauseIsCausedByRetryOther() {
        RetryableException retryableException = createRetryableException(QosException.retryOther(createUrl()));
        assertThat(FastFailoverProxy.isCausedByRetryOther(retryableException)).isTrue();
    }

    @Test
    public void retryableExceptionWithRetryOtherAsIndirectCauseIsCausedByRetryOther() {
        SafeIoException safeIoException = new SafeIoException("Some I/O problem", QosException.retryOther(createUrl()));
        RetryableException retryableException = createRetryableException(safeIoException);
        assertThat(FastFailoverProxy.isCausedByRetryOther(retryableException)).isTrue();
    }

    private void setUpClock() {
        when(clock.instant()).thenAnswer(_$ -> Instant.ofEpochMilli(time.getAndAdd(1_000)));
    }

    private void createProxy() {
        proxy = FastFailoverProxy.newProxyInstance(BinaryOperator.class, () -> binaryOperator, clock);
    }

    private static RetryableException createRetryableException(Throwable throwable) {
        return new RetryableException("foo", throwable, Date.from(Instant.EPOCH));
    }

    private static URL createUrl() {
        try {
            return new URL("https", "bla", "path");
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}
