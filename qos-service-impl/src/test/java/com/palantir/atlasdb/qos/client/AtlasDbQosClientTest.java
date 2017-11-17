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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

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
        when(weigher.weigh(any(), anyLong())).thenReturn(ACTUAL_WEIGHT);
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

        verify(metrics).recordRead(ACTUAL_WEIGHT);
        verifyNoMoreInteractions(metrics);
    }

    @Test
    public void passesResultAndTimeToReadWeigher() throws TestCheckedException {
        qosClient.executeRead(() -> "foo", weigher);

        verify(weigher).weigh("foo", TOTAL_NANOS);
    }

    @Test
    public void consumesSpecifiedNumUnitsForWrites() {
        qosClient.executeWrite(() -> { }, weigher);

        verify(writeLimiter).consumeWithBackoff(ESTIMATED_BYTES);
        verify(writeLimiter).recordAdjustment(ACTUAL_BYTES - ESTIMATED_BYTES);
        verifyNoMoreInteractions(readLimiter, writeLimiter);
    }

    @Test
    public void recordsWriteMetrics() throws TestCheckedException {
        qosClient.executeWrite(() -> { }, weigher);

        verify(metrics).recordWrite(ACTUAL_WEIGHT);
        verifyNoMoreInteractions(metrics);
    }

    @Test
    public void propagatesCheckedExceptions() throws TestCheckedException {
        assertThatThrownBy(() -> qosClient.executeRead(() -> {
            throw new TestCheckedException();
        }, weigher)).isInstanceOf(TestCheckedException.class);

        assertThatThrownBy(() -> qosClient.executeWrite(() -> {
            throw new TestCheckedException();
        }, weigher)).isInstanceOf(TestCheckedException.class);
    }

    static class TestCheckedException extends Exception {}
}
