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
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import feign.RetryableException;

public class FailoverFeignTargetTest {
    private static final int FAILOVERS = 1000;
    private static final int CLUSTER_SIZE = 3;

    private static final String SERVER_1 = "server1";
    private static final String SERVER_2 = "server2";
    private static final String SERVER_3 = "server3";
    private static final List<String> SERVERS = ImmutableList.of(SERVER_1, SERVER_2, SERVER_3);

    private static final RetryableException FAST_FAILOVER_EXCEPTION = mock(RetryableException.class);
    private static final RetryableException NON_FAST_FAILOVER_EXCEPTION = mock(RetryableException.class);
    public static final String MESSAGE = "foo";

    private final FailoverFeignTarget<Object> defaultSemanticsTarget = new FailoverFeignTarget<>(
            SERVERS, 1, Object.class, RetrySemantics.DEFAULT);
    private final FailoverFeignTarget<Object> onlyNonLeadersSemanticsTarget = new FailoverFeignTarget<>(
            SERVERS, 1, Object.class, RetrySemantics.NEVER_EXCEPT_ON_NON_LEADERS);

    static {
        when(FAST_FAILOVER_EXCEPTION.retryAfter()).thenReturn(Date.valueOf(LocalDate.MAX));
        when(NON_FAST_FAILOVER_EXCEPTION.retryAfter()).thenReturn(null);
    }

    @Test
    public void retryableExceptionWithoutRetryAfterIsNotFastFailover() {
        assertThat(FailoverFeignTarget.isFastFailoverException(new RetryableException(MESSAGE, null, null)))
                .isFalse();
    }

    @Test
    public void retryableExceptionWithRetryAfterIsFastFailover() {
        assertThat(FailoverFeignTarget.isFastFailoverException(
                new RetryableException(MESSAGE, null, Date.from(Instant.EPOCH))))
                .isTrue();
    }

    @Test
    public void potentialFollowerExceptionWithoutRetryIsFastFailover() {
        assertThat(FailoverFeignTarget.isFastFailoverException(new PotentialFollowerException(MESSAGE, null, null)))
                .isTrue();
    }

    @Test
    public void failsOverOnFastFailoverException() {
        String initialUrl = defaultSemanticsTarget.url();
        defaultSemanticsTarget.continueOrPropagate(FAST_FAILOVER_EXCEPTION);
        assertThat(defaultSemanticsTarget.url()).isNotEqualTo(initialUrl);
    }

    @Test
    public void failsOverMultipleTimesOnFastFailoverException() {
        String previousUrl;
        for (int i = 0; i < FAILOVERS; i++) {
            previousUrl = defaultSemanticsTarget.url();
            defaultSemanticsTarget.continueOrPropagate(FAST_FAILOVER_EXCEPTION);
            assertThat(defaultSemanticsTarget.url()).isNotEqualTo(previousUrl);
        }
    }

    @Test
    public void failsOverMultipleTimesWithFailingLeader() {
        for (int i = 0; i < FAILOVERS; i++) {
            defaultSemanticsTarget.continueOrPropagate(
                    i % CLUSTER_SIZE == 0 ? NON_FAST_FAILOVER_EXCEPTION : FAST_FAILOVER_EXCEPTION);
        }
    }

    @Test
    public void doesNotImmediatelyFailOverOnNonFastFailoverException() {
        String initialUrl = defaultSemanticsTarget.url();
        defaultSemanticsTarget.continueOrPropagate(NON_FAST_FAILOVER_EXCEPTION);
        assertThat(defaultSemanticsTarget.url()).isEqualTo(initialUrl);
    }

    @Test
    public void rethrowsNonFastFailoverExceptionAfterExceedingRetryLimit() {
        assertThatThrownBy(() -> {
            for (int i = 0; i < FAILOVERS; i++) {
                defaultSemanticsTarget.url();
                defaultSemanticsTarget.continueOrPropagate(NON_FAST_FAILOVER_EXCEPTION);
            }
        }).isEqualTo(NON_FAST_FAILOVER_EXCEPTION);
    }

    @Test
    public void failsOverOnFastFailoverExceptionEvenIfOnlyRetryingOnNonLeaders() {
        String initialUrl = onlyNonLeadersSemanticsTarget.url();
        onlyNonLeadersSemanticsTarget.continueOrPropagate(FAST_FAILOVER_EXCEPTION);
        assertThat(onlyNonLeadersSemanticsTarget.url()).isNotEqualTo(initialUrl);
    }

    @Test
    public void failsOverMultipleTimesOnFastFailoverExceptionEvenIfOnlyRetryingOnNonLeaders() {
        String previousUrl;
        for (int i = 0; i < FAILOVERS; i++) {
            previousUrl = onlyNonLeadersSemanticsTarget.url();
            onlyNonLeadersSemanticsTarget.continueOrPropagate(FAST_FAILOVER_EXCEPTION);
            assertThat(onlyNonLeadersSemanticsTarget.url()).isNotEqualTo(previousUrl);
        }
    }

    @Test
    public void rethrowsNonFastFailoverExceptionIfOnlyRetryingOnNonLeaders() {
        assertThatThrownBy(() -> onlyNonLeadersSemanticsTarget.continueOrPropagate(NON_FAST_FAILOVER_EXCEPTION))
                .isEqualTo(NON_FAST_FAILOVER_EXCEPTION);
    }

    @Test
    public void rethrowsNonFastFailoverExceptionAfterMultipleFastFailoverExceptionsIfOnlyRetryingOnNonLeaders() {
        for (int i = 0; i < FAILOVERS; i++) {
            onlyNonLeadersSemanticsTarget.continueOrPropagate(FAST_FAILOVER_EXCEPTION);
        }
        assertThatThrownBy(() -> onlyNonLeadersSemanticsTarget.continueOrPropagate(NON_FAST_FAILOVER_EXCEPTION))
                .isEqualTo(NON_FAST_FAILOVER_EXCEPTION);
    }
}
