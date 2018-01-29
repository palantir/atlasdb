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
package com.palantir.atlasdb.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.longThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Date;
import java.time.LocalDate;
import java.util.List;

import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.internal.matchers.And;
import org.mockito.internal.matchers.GreaterOrEqual;
import org.mockito.internal.matchers.LessThan;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.http.errors.AtlasDbRemoteException;
import com.palantir.lock.remoting.BlockingTimeoutException;
import com.palantir.remoting2.errors.RemoteException;
import com.palantir.remoting2.errors.SerializableError;

import feign.RetryableException;

public class FailoverFeignTargetTest {
    private static final int FAILOVERS = 1000;
    private static final int ITERATIONS = 100;
    private static final int CLUSTER_SIZE = 3;
    private static final double GOLDEN_RATIO = (Math.sqrt(5) + 1.0) / 2.0;

    private static final String SERVER_1 = "server1";
    private static final String SERVER_2 = "server2";
    private static final String SERVER_3 = "server3";
    private static final List<String> SERVERS = ImmutableList.of(SERVER_1, SERVER_2, SERVER_3);

    private static final RetryableException EXCEPTION_WITH_RETRY_AFTER = mock(RetryableException.class);
    private static final RetryableException EXCEPTION_WITHOUT_RETRY_AFTER = mock(RetryableException.class);
    private static final RetryableException BLOCKING_TIMEOUT_EXCEPTION = mock(RetryableException.class);

    private static final long LOWER_BACKOFF_BOUND = FailoverFeignTarget.BACKOFF_BEFORE_ROUND_ROBIN_RETRY_MILLIS / 2;
    private static final long UPPER_BACKOFF_BOUND =
            (FailoverFeignTarget.BACKOFF_BEFORE_ROUND_ROBIN_RETRY_MILLIS * 3) / 2;

    private FailoverFeignTarget<Object> normalTarget;
    private FailoverFeignTarget<Object> spiedTarget;

    static {
        when(EXCEPTION_WITH_RETRY_AFTER.retryAfter()).thenReturn(Date.valueOf(LocalDate.MAX));
        when(EXCEPTION_WITHOUT_RETRY_AFTER.retryAfter()).thenReturn(null);
        when(BLOCKING_TIMEOUT_EXCEPTION.getCause()).thenReturn(
                new AtlasDbRemoteException(new RemoteException(
                        SerializableError.of("foo", BlockingTimeoutException.class),
                        HttpStatus.SC_SERVICE_UNAVAILABLE)));
    }

    @Before
    public void setup() {
        normalTarget = new FailoverFeignTarget<>(SERVERS, 1, Object.class);
        spiedTarget = spy(new FailoverFeignTarget<>(SERVERS, 100, Object.class));
    }

    @Test
    public void failsOverOnExceptionWithRetryAfter() {
        String initialUrl = normalTarget.url();
        normalTarget.continueOrPropagate(EXCEPTION_WITH_RETRY_AFTER);
        assertThat(normalTarget.url()).isNotEqualTo(initialUrl);
    }

    @Test
    public void failsOverMultipleTimesOnNonBlockingExceptionsWithRetryAfter() {
        String previousUrl;
        for (int i = 0; i < FAILOVERS; i++) {
            previousUrl = normalTarget.url();
            normalTarget.continueOrPropagate(EXCEPTION_WITH_RETRY_AFTER);
            assertThat(normalTarget.url()).isNotEqualTo(previousUrl);
        }
    }

    @Test
    public void doesNotImmediatelyFailOverOnExceptionWithoutRetryAfter() {
        String initialUrl = normalTarget.url();
        normalTarget.continueOrPropagate(EXCEPTION_WITHOUT_RETRY_AFTER);
        assertThat(normalTarget.url()).isEqualTo(initialUrl);
    }

    @Test
    public void rethrowsExceptionWithoutRetryAfterWhenLimitExceeded() {
        assertThatThrownBy(() -> {
            for (int i = 0; i < FAILOVERS; i++) {
                simulateRequest(normalTarget);
                normalTarget.continueOrPropagate(EXCEPTION_WITHOUT_RETRY_AFTER);
            }
        }).isEqualTo(EXCEPTION_WITHOUT_RETRY_AFTER);
    }

    @Test
    public void doesNotFailOverOnBlockingTimeoutException() {
        String initialUrl = normalTarget.url();
        normalTarget.continueOrPropagate(BLOCKING_TIMEOUT_EXCEPTION);
        assertThat(normalTarget.url()).isEqualTo(initialUrl);
    }

    @Test
    public void doesNotFailOverOnMultipleBlockingTimeoutExceptions() {
        String initialUrl = normalTarget.url();
        for (int i = 0; i < FAILOVERS; i++) {
            normalTarget.continueOrPropagate(BLOCKING_TIMEOUT_EXCEPTION);
            assertThat(normalTarget.url()).isEqualTo(initialUrl);
        }
    }

    @Test
    public void failsOverMultipleTimesWithFailingLeader() {
        String initialUrl = normalTarget.url();
        for (int i = 0; i < FAILOVERS; i++) {
            // The 'leader' is the initial node, and fails with non fast-failover exceptions (so without retry after).
            // The other nodes fail with retry afters.
            normalTarget.continueOrPropagate(
                    normalTarget.url().equals(initialUrl) ? EXCEPTION_WITHOUT_RETRY_AFTER : EXCEPTION_WITH_RETRY_AFTER);
        }
    }

    @Test
    public void retriesOnSameNodeIfBlockingTimeoutIsLastAllowedFailureBeforeSwitch() {
        for (int i = 1; i < normalTarget.failuresBeforeSwitching; i++) {
            simulateRequest(normalTarget);
            normalTarget.continueOrPropagate(EXCEPTION_WITHOUT_RETRY_AFTER);
        }
        String currentUrl = normalTarget.url();
        normalTarget.continueOrPropagate(BLOCKING_TIMEOUT_EXCEPTION);
        assertThat(normalTarget.url()).isEqualTo(currentUrl);
    }

    @Test
    public void blockingTimeoutExceptionResetsFailureCount() {
        String currentUrl = normalTarget.url();
        for (int i = 0; i < ITERATIONS; i++) {
            for (int j = 1; j < normalTarget.failuresBeforeSwitching; j++) {
                simulateRequest(normalTarget);
                normalTarget.continueOrPropagate(EXCEPTION_WITHOUT_RETRY_AFTER);
            }
            normalTarget.continueOrPropagate(BLOCKING_TIMEOUT_EXCEPTION);
            assertThat(normalTarget.url()).isEqualTo(currentUrl);
        }
    }

    @Test
    public void exceptionsWithRetryAfterBacksOffAfterQueryingAllNodesInTheCluster() {
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            simulateRequest(spiedTarget);
            spiedTarget.continueOrPropagate(EXCEPTION_WITH_RETRY_AFTER);
        }

        verify(spiedTarget, times(1)).pauseForBackoff(any(),
                longThat(isWithinBounds(LOWER_BACKOFF_BOUND, UPPER_BACKOFF_BOUND)));
    }

    @Test
    public void multipleExceptionsWithRetryAfterBackOffAfterQueryingAllNodesInTheCluster() {
        for (int i = 0; i < 3 * CLUSTER_SIZE; i++) {
            simulateRequest(spiedTarget);
            spiedTarget.continueOrPropagate(EXCEPTION_WITH_RETRY_AFTER);
        }

        verify(spiedTarget, times(3)).pauseForBackoff(any(),
                longThat(isWithinBounds(LOWER_BACKOFF_BOUND, UPPER_BACKOFF_BOUND)));
    }

    @Test
    public void blockingTimeoutExceptionsDoNotBackoff() {
        for (int i = 0; i < ITERATIONS; i++) {
            simulateRequest(spiedTarget);
            spiedTarget.continueOrPropagate(BLOCKING_TIMEOUT_EXCEPTION);

            int expectedNumOfCalls = i + 1;
            verify(spiedTarget, times(expectedNumOfCalls)).pauseForBackoff(any(), eq(0L));
        }
    }

    @Test
    public void exceptionsWithoutRetryAfterBackoffExponentially() {
        int numIterations = 10;

        for (int i = 0; i < numIterations; i++) {
            simulateRequest(spiedTarget);
            spiedTarget.continueOrPropagate(EXCEPTION_WITHOUT_RETRY_AFTER);

            int expectedNumOfCalls = i + 1;
            long cap = Math.round(Math.pow(GOLDEN_RATIO, expectedNumOfCalls));
            verify(spiedTarget, times(expectedNumOfCalls)).pauseForBackoff(any(),
                    longThat(isWithinBounds(0L, cap)));
        }
    }

    private void simulateRequest(FailoverFeignTarget target) {
        // This method is called as a part of a request being invoked.
        // We need to update the mostRecentServerIndex, for the FailoverFeignTarget to track failures properly.
        target.url();
    }

    @SuppressWarnings("unchecked")
    private ArgumentMatcher<Long> isWithinBounds(long lowerBoundInclusive, long upperBoundExclusive) {
        return new And(ImmutableList.of(
                new GreaterOrEqual<>(lowerBoundInclusive),
                new LessThan<>(upperBoundExclusive)
        ));
    }
}
