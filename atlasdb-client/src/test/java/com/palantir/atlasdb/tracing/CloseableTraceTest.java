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
package com.palantir.atlasdb.tracing;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;

import com.palantir.tracing.AlwaysSampler;
import com.palantir.tracing.Tracer;
import com.palantir.tracing.api.Span;
import java.util.List;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CloseableTraceTest {

    private static final String NAME = CloseableTraceTest.class.getName();
    private TestSpanObserver observer;

    @Before
    public void before() throws Exception {
        observer = new TestSpanObserver();
        Tracer.initTrace(Optional.of(true), getClass().getSimpleName() + "." + Math.random());
        Tracer.subscribe(NAME, observer);
    }

    @After
    public void after() throws Exception {
        Tracer.unsubscribe(NAME);
        Tracer.setSampler(AlwaysSampler.INSTANCE);
        Tracer.completeSpan();
    }

    @Test
    public void noOp() throws Exception {
        assertSame(CloseableTrace.noOp(), CloseableTrace.noOp());
    }

    @Test
    public void startLocalTrace() throws Exception {
        try (CloseableTrace trace = CloseableTrace.startLocalTrace("service", "method({})", "foo")) {
            assertNotNull(trace);
        }
        List<Span> spans = observer.spans();
        assertThat(spans, hasSize(1));
        assertThat(spans.get(0).getOperation(), equalTo("service.method(foo)"));
    }

    @Test
    public void startLocalTraceWhileNotSampling() throws Exception {
        Tracer.initTrace(Optional.of(false), "abc");
        assertFalse(Tracer.isTraceObservable());
        try (CloseableTrace trace = CloseableTrace.startLocalTrace("service", "method({})", "foo")) {
            assertNotNull(trace);
        }
        List<Span> spans = observer.spans();
        assertThat("Expected empty spans: " + spans, spans, hasSize(0));
    }
}
