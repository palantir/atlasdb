/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.qos.client;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.qos.ImmutableQueryWeight;
import com.palantir.atlasdb.qos.QosClient;
import com.palantir.atlasdb.qos.QueryWeight;
import com.palantir.atlasdb.qos.metrics.QosMetrics;
import com.palantir.atlasdb.qos.ratelimit.ImmutableQosRateLimiters;
import com.palantir.atlasdb.qos.ratelimit.QosRateLimiter;
import com.palantir.atlasdb.qos.ratelimit.QosRateLimiters;
import com.palantir.remoting.api.errors.QosException;

@RunWith(Parameterized.class)
public final class AtlasDbQosClientTest {

    private static final int ESTIMATED_BYTES = 10;
    private static final long START_NANOS = 1100L;
    private static final long END_NANOS = 5500L;
    private static final long TOTAL_NANOS = END_NANOS - START_NANOS;

    private static final QueryWeight ESTIMATED_WEIGHT = ImmutableQueryWeight.builder()
            .numBytes(ESTIMATED_BYTES)
            .numDistinctRows(1)
            .timeTakenNanos((int) TOTAL_NANOS)
            .build();

    private final int actualBytes;

    private final QueryWeight actualWeight;

    private QosClient.QueryWeigher weigher = mock(QosClient.QueryWeigher.class);

    private QosRateLimiter readLimiter = mock(QosRateLimiter.class);
    private QosRateLimiter writeLimiter = mock(QosRateLimiter.class);
    private QosRateLimiters rateLimiters = ImmutableQosRateLimiters.builder()
            .read(readLimiter).write(writeLimiter).build();
    private QosMetrics metrics = mock(QosMetrics.class);
    private Ticker ticker = mock(Ticker.class);

    private AtlasDbQosClient qosClient = new AtlasDbQosClient(rateLimiters, metrics, ticker);

    @Parameterized.Parameters
    public static Collection<Integer> actualBytes() {
        return ImmutableList.of(51, 5);
    }

    public AtlasDbQosClientTest(int actualBytes) {
        this.actualBytes = actualBytes;
        this.actualWeight = ImmutableQueryWeight.builder()
                .numBytes(actualBytes)
                .numDistinctRows(10)
                .timeTakenNanos((int) TOTAL_NANOS)
                .build();
    }

    @Before
    public void setUp() {
        when(ticker.read()).thenReturn(START_NANOS).thenReturn(END_NANOS);

        when(weigher.estimate()).thenReturn(ESTIMATED_WEIGHT);
        when(weigher.weighSuccess(any(), anyLong())).thenReturn(actualWeight);
        when(weigher.weighFailure(any(), anyLong())).thenReturn(actualWeight);

        when(readLimiter.consumeWithBackoff(anyLong())).thenReturn(Duration.ZERO);
        when(writeLimiter.consumeWithBackoff(anyLong())).thenReturn(Duration.ZERO);
    }

    @Test
    public void consumesSpecifiedNumUnitsForReads() {
        qosClient.executeRead(() -> "foo", weigher);

        verify(readLimiter).consumeWithBackoff(ESTIMATED_BYTES);
        verify(readLimiter).recordAdjustment(actualBytes - ESTIMATED_BYTES);
        verifyNoMoreInteractions(readLimiter, writeLimiter);
    }

    @Test
    public void recordsReadMetrics() throws TestCheckedException {
        qosClient.executeRead(() -> "foo", weigher);

        verify(metrics).recordReadEstimate(ESTIMATED_WEIGHT);
        verify(metrics).recordRead(actualWeight);
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
        verify(writeLimiter).recordAdjustment(actualBytes - ESTIMATED_BYTES);
        verifyNoMoreInteractions(readLimiter, writeLimiter);
    }

    @Test
    public void recordsWriteMetrics() throws TestCheckedException {
        qosClient.executeWrite(() -> null, weigher);

        verify(metrics).recordWrite(actualWeight);
        verify(metrics, never()).recordReadEstimate(any());
    }

    @Test
    public void recordsReadMetricsOnFailure() throws TestCheckedException {
        TestCheckedException error = new TestCheckedException();
        assertThatThrownBy(() -> qosClient.executeRead(() -> {
            throw error;
        }, weigher)).isInstanceOf(TestCheckedException.class);

        verify(metrics).recordRead(actualWeight);
    }

    @Test
    public void recordsWriteMetricsOnFailure() throws TestCheckedException {
        TestCheckedException error = new TestCheckedException();
        assertThatThrownBy(() -> qosClient.executeWrite(() -> {
            throw error;
        }, weigher)).isInstanceOf(TestCheckedException.class);

        verify(metrics).recordWrite(actualWeight);
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

        verify(metrics, never()).recordThrottleExceptions();
    }

    @Test
    public void recordsBackoffTime() {
        when(readLimiter.consumeWithBackoff(anyLong())).thenReturn(Duration.ofMillis(1_100));
        qosClient.executeRead(() -> "foo", weigher);

        verify(metrics).recordBackoffMicros(1_100_000);
    }

    @Test
    public void recordsBackoffExceptions() {
        when(readLimiter.consumeWithBackoff(anyLong())).thenThrow(QosException.throttle());
        assertThatThrownBy(() -> qosClient.executeRead(() -> "foo", weigher)).isInstanceOf(
                QosException.Throttle.class);

        verify(metrics).recordThrottleExceptions();
    }

    @Test
    public void doesNotRecordRuntimeExceptions() {
        when(readLimiter.consumeWithBackoff(anyLong())).thenThrow(new RuntimeException("foo"));
        assertThatThrownBy(() -> qosClient.executeRead(() -> "foo", weigher)).isInstanceOf(
                RuntimeException.class);

        verify(metrics, never()).recordThrottleExceptions();
    }

    static class TestCheckedException extends Exception {}
}
