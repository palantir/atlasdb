/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.autobatch;

import com.lmax.disruptor.EventHandler;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher.DisruptorFuture;
import com.palantir.tracing.RenderTracingRule;
import com.palantir.tracing.Tracers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TracingEventHandlerTest {

    @Rule
    public RenderTracingRule renderTracingRule = new RenderTracingRule();

    private final EventHandler<BatchElement<Integer, Long>> delegate = new FutureCompletingEventHandler();

    @Test
    public void flushesHaveTraces() throws Exception {
        TracingEventHandler<Integer, Long> tracingHandler = new TracingEventHandler<>(delegate, 10);

        DisruptorFuture<Long> eventFuture = Tracers.wrapListenableFuture("test", () -> {
            TestBatchElement element = new TestBatchElement();
            try {
                tracingHandler.onEvent(element, 45, true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return element.result();
        });

        eventFuture.get();
    }

    private static final class TestBatchElement implements BatchElement<Integer, Long> {

        private final DisruptorFuture<Long> future = new DisruptorFuture<>("test");

        @Override
        public Integer argument() {
            return 5;
        }

        @Override
        public DisruptorFuture<Long> result() {
            return future;
        }
    }

    private static final class FutureCompletingEventHandler implements EventHandler<BatchElement<Integer, Long>> {

        @Override
        public void onEvent(BatchElement<Integer, Long> event, long _sequence, boolean endOfBatch) throws Exception {
            if (endOfBatch) {
                event.result().set(5L);
            }
        }
    }
}
