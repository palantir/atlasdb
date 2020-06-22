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

package com.palantir.timestamp;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.immutables.value.Value;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class RequestBatchingTimestampServiceTest {
    private static final int MAX_TIMESTAMPS = 10_000;

    @Spy private final TimestampService unbatchedDelegate = new MaxTimestampsToGiveTimestampService();

    private CloseableTimestampService timestamp;

    @Before
    public void before() {
        timestamp = RequestBatchingTimestampService.create(unbatchedDelegate);
    }

    @After
    public void after() {
        timestamp.close();
    }

    @Test
    public void delegatesInitializationCheck() {
        TimestampService delegate = mock(TimestampService.class);
        RequestBatchingTimestampService service = RequestBatchingTimestampService.create(delegate);

        when(delegate.isInitialized())
                .thenReturn(false)
                .thenReturn(true);

        assertFalse(service.isInitialized());
        assertTrue(service.isInitialized());
    }

    @Test
    public void throwsIfAskForZeroTimestamps() {
        assertThatThrownBy(() -> timestamp.getFreshTimestamps(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Must not request zero or negative timestamps");
    }

    @Test
    public void coalescesRequestsTogether() {
        assertThat(requestBatches(1, 2, 3)).containsExactly(single(1), range(2, 4), range(4, 7));
        verify(unbatchedDelegate).getFreshTimestamps(6);
        verifyNoMoreInteractions(unbatchedDelegate);
    }

    @Test
    public void handsOutMaximumNumberOfTimestampsIfLimited() {
        assertThat(requestBatches(1, 20_000, 3))
                .containsExactly(single(1), range(10_001, 20_001), range(20_001, 20_004));
        verify(unbatchedDelegate).getFreshTimestamps(20_004);
        verify(unbatchedDelegate).getFreshTimestamps(20_003);
        verify(unbatchedDelegate).getFreshTimestamps(3);
        verifyNoMoreInteractions(unbatchedDelegate);
    }

    private List<TimestampRange> requestBatches(int... sizes) {
        List<BatchElement<Integer, TimestampRange>> elements = Arrays.stream(sizes)
                .mapToObj(size -> ImmutableTestBatchElement.builder()
                        .argument(size)
                        .result(new DisruptorAutobatcher.DisruptorFuture<>("test"))
                        .build())
                .collect(toList());
        RequestBatchingTimestampService.consumer(unbatchedDelegate).accept(elements);
        return Futures.getUnchecked(Futures.allAsList(Lists.transform(elements, BatchElement::result)));
    }

    private static TimestampRange single(int ts) {
        return TimestampRange.createInclusiveRange(ts, ts);
    }

    private static TimestampRange range(int start, int end) {
        return TimestampRange.createInclusiveRange(start, end - 1);
    }

    @Value.Immutable
    interface TestBatchElement extends BatchElement<Integer, TimestampRange> {}

    private static class MaxTimestampsToGiveTimestampService implements TimestampService {
        private final AtomicLong counter = new AtomicLong(0);

        @Override
        public long getFreshTimestamp() {
            return counter.incrementAndGet();
        }

        @Override
        public TimestampRange getFreshTimestamps(int timestampsToGet) {
            if (timestampsToGet <= 0) {
                throw new IllegalArgumentException("Argument must be positive: " + timestampsToGet);
            }
            long timestampsToRequest = Math.min(timestampsToGet, MAX_TIMESTAMPS);
            long topValue = counter.addAndGet(timestampsToRequest);
            return TimestampRange.createInclusiveRange(topValue - timestampsToRequest + 1, topValue);
        }
    }
}
