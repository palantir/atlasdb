/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.function.BinaryOperator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RetryOnSocketTimeoutExceptionProxyTest {

    @Mock
    private BinaryOperator<Integer> binaryOperator;

    @Mock
    private BinaryOperator<Integer> binaryOperator2;

    private BinaryOperator<Integer> proxy;

    @Before
    public void setUp() {
        createProxy();
    }

    @Test
    public void isSocketTimeoutException() {
        assertThat(RetryOnSocketTimeoutExceptionProxy.isCausedBySocketTimeoutException(new SocketTimeoutException()))
                .isTrue();
    }

    @Test
    public void isNotSocketTimeoutException() {
        assertThat(RetryOnSocketTimeoutExceptionProxy.isCausedBySocketTimeoutException(new SocketException()))
                .isFalse();
    }

    @Test
    public void noFailures() {
        when(binaryOperator.apply(1, 2)).thenReturn(3);
        assertThat(proxy.apply(1, 2)).isEqualTo(3);
    }

    @Test
    public void oneFailure() {
        RuntimeException socketTimeoutException = new RuntimeException(new SocketTimeoutException());
        when(binaryOperator.apply(1, 2)).thenThrow(socketTimeoutException).thenReturn(3);
        assertThat(proxy.apply(1, 2)).isEqualTo(3);
    }

    @Test
    public void twoFailures() {
        RuntimeException socketTimeoutException = new RuntimeException(new SocketTimeoutException());
        when(binaryOperator.apply(1, 2))
                .thenThrow(socketTimeoutException)
                .thenThrow(socketTimeoutException)
                .thenReturn(3);
        assertThat(proxy.apply(1, 2)).isEqualTo(3);
    }

    @Test
    public void fourFailures() {
        RuntimeException socketTimeoutException = new RuntimeException(new SocketTimeoutException());
        when(binaryOperator.apply(1, 2))
                .thenThrow(socketTimeoutException)
                .thenThrow(socketTimeoutException)
                .thenThrow(socketTimeoutException)
                .thenThrow(socketTimeoutException)
                .thenReturn(3);
        assertThat(proxy.apply(1, 2)).isEqualTo(3);
    }

    @Test
    public void sixFailures() {
        RuntimeException socketTimeoutException = new RuntimeException(new SocketTimeoutException());
        when(binaryOperator.apply(1, 2))
                .thenThrow(socketTimeoutException)
                .thenThrow(socketTimeoutException)
                .thenThrow(socketTimeoutException)
                .thenThrow(socketTimeoutException)
                .thenThrow(socketTimeoutException)
                .thenThrow(socketTimeoutException)
                .thenReturn(3);

        assertThatThrownBy(() -> proxy.apply(1, 2)).isEqualTo(socketTimeoutException);
    }

    @Test
    public void differentFailure() {
        RuntimeException socketException = new RuntimeException(new SocketException());
        when(binaryOperator.apply(1, 2)).thenThrow(socketException).thenReturn(3);
        assertThatThrownBy(() -> proxy.apply(1, 2)).isEqualTo(socketException);
    }

    @Test
    public void retryThenDifferentFailure() {
        RuntimeException socketTimeoutException = new RuntimeException(new SocketTimeoutException());
        RuntimeException socketException = new RuntimeException(new SocketException());
        when(binaryOperator.apply(1, 2))
                .thenThrow(socketTimeoutException)
                .thenThrow(socketException)
                .thenReturn(3);
        assertThatThrownBy(() -> proxy.apply(1, 2)).isEqualTo(socketException);
    }

    @Test
    public void twoProxies() {
        BinaryOperator<Integer> proxy2 = (BinaryOperator<Integer>)
                RetryOnSocketTimeoutExceptionProxy.newProxyInstance(BinaryOperator.class, () -> binaryOperator2);

        RuntimeException socketTimeoutException = new RuntimeException(new SocketTimeoutException());

        when(binaryOperator.apply(1, 2))
                .thenThrow(socketTimeoutException)
                .thenThrow(socketTimeoutException)
                .thenThrow(socketTimeoutException)
                .thenThrow(socketTimeoutException)
                .thenThrow(socketTimeoutException)
                .thenReturn(3);

        when(binaryOperator2.apply(3, 4))
                .thenThrow(socketTimeoutException)
                .thenThrow(socketTimeoutException)
                .thenThrow(socketTimeoutException)
                .thenThrow(socketTimeoutException)
                .thenReturn(5);

        assertThat(proxy.apply(1, 2)).isEqualTo(3);
        assertThat(proxy2.apply(3, 4)).isEqualTo(5);
    }

    private void createProxy() {
        proxy = (BinaryOperator<Integer>)
                RetryOnSocketTimeoutExceptionProxy.newProxyInstance(BinaryOperator.class, () -> binaryOperator);
    }
}
