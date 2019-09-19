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
package com.palantir.atlasdb.keyvalue.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.tracing.Tracer;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanObserver;
import com.palantir.tracing.api.SpanType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TracingKvsTest extends AbstractKeyValueServiceTest {
    @ClassRule
    public static final TestResourceManager TRM = new TestResourceManager(() ->
            TracingKeyValueService.create(new InMemoryKeyValueService(false)));

    private static final Logger log = LoggerFactory.getLogger(TracingKvsTest.class);
    private static final String TEST_OBSERVER_NAME = TracingKvsTest.class.getName();

    public TracingKvsTest() {
        super(TRM);
    }

    @Override
    public void setUp() throws Exception {
        Tracer.initTrace(Optional.of(true), getClass().getSimpleName() + "." + Math.random());
        Tracer.subscribe(TEST_OBSERVER_NAME, new TestSpanObserver());
        Tracer.startSpan("test", SpanType.LOCAL);
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            Optional<Span> finishedSpan = Tracer.completeSpan();
            SpanObserver observer = Tracer.unsubscribe(TEST_OBSERVER_NAME);
            assertThat(observer).isInstanceOf(TestSpanObserver.class);
            List<Span> spans = ((TestSpanObserver) observer).spans();
            log.warn("{} spans: {}", spans.size(), spans.stream().map(Span::getOperation).collect(Collectors.toList()));
            if (Tracer.isTraceObservable()) {
                assertThat(finishedSpan.isPresent()).isTrue();
                assertThat(finishedSpan.get().getOperation()).isEqualTo("test");
                String traceId = finishedSpan.get().getTraceId();
                assertThat(traceId).isNotNull();
                assertThat(traceId).isNotEmpty();
                assertThat(spans).isNotEmpty();
                assertThat(spans.size())
                        .describedAs("Should include root test span and additional KVS method spans %s", spans)
                        .isGreaterThanOrEqualTo(1);
                assertThat(spans.stream()
                        .filter(span -> !Objects.equals(traceId, span.getTraceId()))
                        .map(Span::getTraceId)
                        .collect(Collectors.toSet()))
                        .describedAs("All spans should have same trace ID %s, spans %s", traceId, spans)
                        .isEmpty();
            }
        } finally {
            super.tearDown();
        }
    }

    private static class TestSpanObserver implements SpanObserver {
        private final List<Span> spans = new ArrayList<>();

        @Override
        public void consume(Span span) {
            log.warn("{}", span);
            spans.add(span);
        }

        List<Span> spans() {
            return Collections.unmodifiableList(spans);
        }
    }

}
