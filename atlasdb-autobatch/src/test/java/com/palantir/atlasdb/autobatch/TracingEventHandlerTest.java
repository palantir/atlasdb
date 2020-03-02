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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.SettableFuture;
import com.lmax.disruptor.EventHandler;
import com.palantir.tracing.Observability;
import com.palantir.tracing.Tracer;
import com.palantir.tracing.api.Span;

@RunWith(MockitoJUnitRunner.class)
public class TracingEventHandlerTest {

    @Rule
    public TraceCapturingRule traceRule = new TraceCapturingRule();

    @Mock
    private EventHandler<BatchElement<Integer, Long>> delegate;
    private static final TestBatchElement ELEMENT = new TestBatchElement();

    @Test
    public void nonFlushesDoNotHaveTraces() throws Exception {
        TracingEventHandler<Integer, Long> tracingHandler =
                new TracingEventHandler<>(delegate, "test", Observability.SAMPLE);

        tracingHandler.onEvent(ELEMENT, 45, false);

        assertThat(traceRule.spansObserved())
                .as("no spans should be emitted")
                .isEmpty();

        verify(delegate).onEvent(ELEMENT, 45, false);
    }

    @Test
    public void flushesHaveTraces() throws Exception {
        TracingEventHandler<Integer, Long> tracingHandler =
                new TracingEventHandler<>(delegate, "test", Observability.SAMPLE);

        tracingHandler.onEvent(ELEMENT, 45, true);

        assertThat(traceRule.spansObserved())
                .as("a flush should emit a span")
                .hasSize(1);

        verify(delegate).onEvent(ELEMENT, 45, true);
    }

    @Test
    public void separateTracePerFlush() throws Exception {
        TracingEventHandler<Integer, Long> tracingHandler =
                new TracingEventHandler<>(delegate, "test", Observability.SAMPLE);

        tracingHandler.onEvent(ELEMENT, 45, true);
        tracingHandler.onEvent(ELEMENT, 47, true);
        assertThat(traceRule.spansObserved())
                .as("a flush should emit a span")
                .hasSize(2);

        verify(delegate).onEvent(ELEMENT, 45, true);
        verify(delegate).onEvent(ELEMENT, 47, true);
    }

    private static class TraceCapturingRule extends ExternalResource {

        private final List<Span> spansObserved = Lists.newArrayList();

        @Override
        protected void before() {
            Tracer.subscribe("trace-rule", spansObserved::add);
        }

        @Override
        protected void after() {
            Tracer.unsubscribe("trace-rule");
        }

        public List<Span> spansObserved() {
            return spansObserved;
        }
    }

    private static class TestBatchElement implements BatchElement<Integer, Long> {

        @Override
        public Integer argument() {
            return 5;
        }

        @Override
        public SettableFuture<Long> result() {
            return SettableFuture.create();
        }
    }
}
