/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Date;
import java.time.LocalDate;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.lock.BlockingTimeoutException;

import feign.RetryableException;

public class FailoverFeignTargetTest {
    private static final int FAILOVERS = 1000;
    private static final int CLUSTER_SIZE = 3;

    private static final String SERVER_1 = "server1";
    private static final String SERVER_2 = "server2";
    private static final String SERVER_3 = "server3";
    private static final List<String> SERVERS = ImmutableList.of(SERVER_1, SERVER_2, SERVER_3);

    private static final RetryableException EXCEPTION_WITH_RETRY_AFTER = mock(RetryableException.class);
    private static final RetryableException EXCEPTION_WITHOUT_RETRY_AFTER = mock(RetryableException.class);
    private static final RetryableException BLOCKING_TIMEOUT_EXCEPTION = mock(RetryableException.class);

    private final FailoverFeignTarget<Object> target = new FailoverFeignTarget<>(
            SERVERS, 1, Object.class);

    static {
        when(EXCEPTION_WITH_RETRY_AFTER.retryAfter()).thenReturn(Date.valueOf(LocalDate.MAX));
        when(EXCEPTION_WITHOUT_RETRY_AFTER.retryAfter()).thenReturn(null);
        when(BLOCKING_TIMEOUT_EXCEPTION.getCause()).thenReturn(new BlockingTimeoutException(new Exception()));
    }

    @Test
    public void failsOverOnExceptionWithoutRetryAfter() {
        String initialUrl = target.url();
        target.continueOrPropagate(EXCEPTION_WITHOUT_RETRY_AFTER);
        assertThat(target.url()).isNotEqualTo(initialUrl);
    }

    @Test
    public void failsOverMultipleTimesOnExceptionsWithoutRetryAfter() {
        String previousUrl;
        for (int i = 0; i < FAILOVERS; i++) {
            previousUrl = target.url();
            target.continueOrPropagate(EXCEPTION_WITHOUT_RETRY_AFTER);
            assertThat(target.url()).isNotEqualTo(previousUrl);
        }
    }

    @Test
    public void doesNotImmediatelyFailOverOnExceptionWithRetryAfter() {
        String initialUrl = target.url();
        target.continueOrPropagate(EXCEPTION_WITH_RETRY_AFTER);
        assertThat(target.url()).isEqualTo(initialUrl);
    }

    @Test
    public void rethrowsExceptionWithRetryAfterWhenLimitExceeded() {
        assertThatThrownBy(() -> {
            for (int i = 0; i < FAILOVERS; i++) {
                target.url();
                target.continueOrPropagate(EXCEPTION_WITH_RETRY_AFTER);
            }
        }).isEqualTo(EXCEPTION_WITH_RETRY_AFTER);
    }

    @Test
    public void doesNotFailOverOnBlockingTimeoutException() {
        String initialUrl = target.url();
        target.continueOrPropagate(BLOCKING_TIMEOUT_EXCEPTION);
        assertThat(target.url()).isEqualTo(initialUrl);
    }

    @Test
    public void doesNotFailOverOnMultipleBlockingTimeoutExceptions() {
        String initialUrl = target.url();
        for (int i = 0; i < FAILOVERS; i++) {
            target.continueOrPropagate(BLOCKING_TIMEOUT_EXCEPTION);
            assertThat(target.url()).isEqualTo(initialUrl);
        }
    }

    @Test
    public void failsOverMultipleTimesWithFailingLeader() {
        for (int i = 0; i < FAILOVERS; i++) {
            target.continueOrPropagate(
                    i % CLUSTER_SIZE == 0 ? EXCEPTION_WITHOUT_RETRY_AFTER : EXCEPTION_WITH_RETRY_AFTER);
        }
    }
}
