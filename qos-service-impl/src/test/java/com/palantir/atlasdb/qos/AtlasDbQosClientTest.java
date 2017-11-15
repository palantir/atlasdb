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

package com.palantir.atlasdb.qos;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Ticker;
import com.palantir.atlasdb.qos.client.AtlasDbQosClient;
import com.palantir.atlasdb.qos.ratelimit.QosRateLimiter;

public class AtlasDbQosClientTest {

    private static final int ESTIMATED_BYTES = 10;
    private static final int ACTUAL_BYTES = 51;
    private static final long START_NANOS = 1100L;
    private static final long END_NANOS = 5500L;
    private static final long TOTAL_TIME_MICROS = 4;

    private QosService qosService = mock(QosService.class);
    private QosRateLimiter rateLimiter = mock(QosRateLimiter.class);
    private QosMetrics metrics = mock(QosMetrics.class);
    private Ticker ticker = mock(Ticker.class);

    private AtlasDbQosClient qosClient = new AtlasDbQosClient(rateLimiter, metrics, ticker);

    @Before
    public void setUp() {
        when(qosService.getLimit("test-client")).thenReturn(100);

        when(ticker.read()).thenReturn(START_NANOS).thenReturn(END_NANOS);
    }

    @Test
    public void consumesSpecifiedNumUnitsForReads() {
        qosClient.executeRead(() -> ESTIMATED_BYTES, () -> "foo", ignored -> ACTUAL_BYTES);

        verify(rateLimiter).consumeWithBackoff(ESTIMATED_BYTES);
        verify(rateLimiter).recordAdjustment(ACTUAL_BYTES - ESTIMATED_BYTES);
        verifyNoMoreInteractions(rateLimiter);
    }

    @Test
    public void recordsReadMetrics() throws TestCheckedException {
        qosClient.executeRead(() -> ESTIMATED_BYTES, () -> "foo", ignored -> ACTUAL_BYTES);

        verify(metrics).updateReadCount();
        verify(metrics).updateBytesRead(ACTUAL_BYTES);
        verify(metrics).updateReadTimeMicros(TOTAL_TIME_MICROS);
        verifyNoMoreInteractions(metrics);
    }

    @Test
    public void consumesSpecifiedNumUnitsForWrites() {
        qosClient.executeWrite(() -> ACTUAL_BYTES, () -> { });

        verify(rateLimiter).consumeWithBackoff(ACTUAL_BYTES);
        verifyNoMoreInteractions(rateLimiter);
    }

    @Test
    public void recordsWriteMetrics() throws TestCheckedException {
        qosClient.executeWrite(() -> ACTUAL_BYTES, () -> { });

        verify(metrics).updateWriteCount();
        verify(metrics).updateBytesWritten(ACTUAL_BYTES);
        verify(metrics).updateWriteTimeMicros(TOTAL_TIME_MICROS);
        verifyNoMoreInteractions(metrics);
    }

    @Test
    public void propagatesCheckedExceptions() throws TestCheckedException {
        assertThatThrownBy(() -> qosClient.executeRead(() -> 1, () -> {
            throw new TestCheckedException();
        }, ignored -> 1)).isInstanceOf(TestCheckedException.class);

        assertThatThrownBy(() -> qosClient.executeWrite(() -> 1, () -> {
            throw new TestCheckedException();
        })).isInstanceOf(TestCheckedException.class);
    }

    static class TestCheckedException extends Exception { }
}
