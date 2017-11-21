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

package com.palantir.atlasdb.qos.client;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Ticker;
import com.palantir.atlasdb.qos.ImmutableQueryWeight;
import com.palantir.atlasdb.qos.QosClient;
import com.palantir.atlasdb.qos.QueryWeight;
import com.palantir.atlasdb.qos.metrics.QosMetrics;
import com.palantir.atlasdb.qos.ratelimit.ImmutableQosRateLimiters;
import com.palantir.atlasdb.qos.ratelimit.QosRateLimiter;
import com.palantir.atlasdb.qos.ratelimit.QosRateLimiters;
import com.palantir.atlasdb.qos.ratelimit.RateLimitExceededException;

public class AtlasDbQosClientTest {

    private static final int ESTIMATED_BYTES = 10;
    private static final int ACTUAL_BYTES = 51;
    private static final long START_NANOS = 1100L;
    private static final long END_NANOS = 5500L;
    private static final long TOTAL_NANOS = END_NANOS - START_NANOS;

    private static final QueryWeight ESTIMATED_WEIGHT = ImmutableQueryWeight.builder()
            .numBytes(ESTIMATED_BYTES)
            .numDistinctRows(1)
            .timeTakenNanos((int) TOTAL_NANOS)
            .build();

    private static final QueryWeight ACTUAL_WEIGHT = ImmutableQueryWeight.builder()
            .numBytes(ACTUAL_BYTES)
            .numDistinctRows(10)
            .timeTakenNanos((int) TOTAL_NANOS)
            .build();

    private QosClient.QueryWeigher weigher = mock(QosClient.QueryWeigher.class);

    private QosRateLimiter readLimiter = mock(QosRateLimiter.class);
    private QosRateLimiter writeLimiter = mock(QosRateLimiter.class);
    private QosRateLimiters rateLimiters = ImmutableQosRateLimiters.builder()
            .read(readLimiter).write(writeLimiter).build();
    private QosMetrics metrics = mock(QosMetrics.class);
    private Ticker ticker = mock(Ticker.class);

    private AtlasDbQosClient qosClient = new AtlasDbQosClient(rateLimiters, metrics, ticker);

    @Before
    public void setUp() {
        when(ticker.read()).thenReturn(START_NANOS).thenReturn(END_NANOS);

        when(weigher.estimate()).thenReturn(ESTIMATED_WEIGHT);
        when(weigher.weighSuccess(any(), anyLong())).thenReturn(ACTUAL_WEIGHT);
        when(weigher.weighFailure(any(), anyLong())).thenReturn(ACTUAL_WEIGHT);

        when(readLimiter.consumeWithBackoff(anyLong())).thenReturn(Duration.ZERO);
        when(writeLimiter.consumeWithBackoff(anyLong())).thenReturn(Duration.ZERO);
    }

    @Test
    public void consumesSpecifiedNumUnitsForReads() {
        qosClient.executeRead(() -> "foo", weigher);

        verify(readLimiter).consumeWithBackoff(ESTIMATED_BYTES);
        verify(readLimiter).recordAdjustment(ACTUAL_BYTES - ESTIMATED_BYTES);
        verifyNoMoreInteractions(readLimiter, writeLimiter);
    }

    @Test
    public void recordsReadMetrics() throws TestCheckedException {
        qosClient.executeRead(() -> "foo", weigher);

        verify(metrics).recordReadEstimate(ESTIMATED_WEIGHT);
        verify(metrics).recordRead(ACTUAL_WEIGHT);
    }

    @Test
    public void passesResultAndTimeToReadWeigher() throws TestCheckedException {
        qosClient.executeRead(() -> "foo", weigher);

        verify(weigher).weighSuccess("foo", TOTAL_NANOS);
    }

    @Test
    public void consumesSpecifiedNumUnitsForWrites() {
        qosClient.executeWrite(() -> null, weigher);

        verify(writeLimiter).consumeWithBackoff(ESTIMATED_BYTES);
        verify(writeLimiter).recordAdjustment(ACTUAL_BYTES - ESTIMATED_BYTES);
        verifyNoMoreInteractions(readLimiter, writeLimiter);
    }

    @Test
    public void recordsWriteMetrics() throws TestCheckedException {
        qosClient.executeWrite(() -> null, weigher);

        verify(metrics).recordWrite(ACTUAL_WEIGHT);
        verify(metrics, never()).recordReadEstimate(any());
    }

    @Test
    public void recordsReadMetricsOnFailure() throws TestCheckedException {
        TestCheckedException error = new TestCheckedException();
        assertThatThrownBy(() -> qosClient.executeRead(() -> {
            throw error;
        }, weigher)).isInstanceOf(TestCheckedException.class);

        verify(metrics).recordRead(ACTUAL_WEIGHT);
    }

    @Test
    public void recordsWriteMetricsOnFailure() throws TestCheckedException {
        TestCheckedException error = new TestCheckedException();
        assertThatThrownBy(() -> qosClient.executeWrite(() -> {
            throw error;
        }, weigher)).isInstanceOf(TestCheckedException.class);

        verify(metrics).recordWrite(ACTUAL_WEIGHT);
    }

    @Test
    public void passesExceptionToWeigherOnFailure() throws TestCheckedException {
        TestCheckedException error = new TestCheckedException();
        assertThatThrownBy(() -> qosClient.executeRead(() -> {
            throw error;
        }, weigher)).isInstanceOf(TestCheckedException.class);

        verify(weigher).weighFailure(error, TOTAL_NANOS);
        verify(weigher, never()).weighSuccess(any(), anyLong());
    }

    @Test
    public void propagatesCheckedExceptions() throws TestCheckedException {
        assertThatThrownBy(() -> qosClient.executeRead(() -> {
            throw new TestCheckedException();
        }, weigher)).isInstanceOf(TestCheckedException.class);

        assertThatThrownBy(() -> qosClient.executeWrite(() -> {
            throw new TestCheckedException();
        }, weigher)).isInstanceOf(TestCheckedException.class);

        verify(metrics, never()).recordRateLimitedException();
    }

    @Test
    public void recordsBackoffTime() {
        when(readLimiter.consumeWithBackoff(anyLong())).thenReturn(Duration.ofMillis(1_100));
        qosClient.executeRead(() -> "foo", weigher);

        verify(metrics).recordBackoffMicros(1_100_000);
    }

    @Test
    public void recordsBackoffExceptions() {
        when(readLimiter.consumeWithBackoff(anyLong())).thenThrow(new RateLimitExceededException("rate limited"));
        assertThatThrownBy(() -> qosClient.executeRead(() -> "foo", weigher)).isInstanceOf(
                RateLimitExceededException.class);

        verify(metrics).recordRateLimitedException();
    }

    @Test
    public void doesNotRecordRuntimeExceptions() {
        when(readLimiter.consumeWithBackoff(anyLong())).thenThrow(new RuntimeException("foo"));
        assertThatThrownBy(() -> qosClient.executeRead(() -> "foo", weigher)).isInstanceOf(
                RuntimeException.class);

        verify(metrics, never()).recordRateLimitedException();
    }

    static class TestCheckedException extends Exception {}
}
