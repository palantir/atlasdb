/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.TaskRunner.Metadata;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.tracing.Observability;
import com.palantir.tracing.Tracer;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanObserver;
import com.palantir.tracing.api.SpanType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class TaskRunnerTest {
    private final TaskRunner taskRunner = new TaskRunner(MoreExecutors.newDirectExecutorService());
    private final Callable<String> succeedingCallable = () -> "test";
    private final Callable<String> failingCallable = () -> {
        throw new SafeRuntimeException("Failed to run");
    };
    private final Metadata succeedingMetadata = setMetadata("successful task");

    private final Metadata failingMetadata = setMetadata("failing task");

    private final TestSpanObserver observer = new TestSpanObserver();

    @Before
    public void before() {
        Tracer.initTraceWithSpan(Observability.SAMPLE, "trace-id-1", "task", SpanType.LOCAL);
        Tracer.subscribe(getClass().getName(), observer);
        assertThat(observer.getMetadata()).isEqualTo(ImmutableList.of());
    }

    @After
    public void after() throws Exception {
        Tracer.unsubscribe(getClass().getName());
    }

    @Test
    public void taskRunsSuccessfully() {
        assertThat(taskRunner.runAllTasksCancelOnFailure(ImmutableList.of(
                        ImmutableKvsLoadingTask.of(succeedingCallable, succeedingMetadata),
                        ImmutableKvsLoadingTask.of(succeedingCallable, succeedingMetadata))))
                .isEqualTo(ImmutableList.of("test", "test"));
        assertThat(observer.getMetadata())
                .isEqualTo(ImmutableList.of(succeedingMetadata.getMetadata(), succeedingMetadata.getMetadata()));
    }

    @Test
    public void failingTaskHandledSuccessfully() {
        assertThatThrownBy(() -> taskRunner.runAllTasksCancelOnFailure(ImmutableList.of(
                        ImmutableKvsLoadingTask.of(succeedingCallable, succeedingMetadata),
                        ImmutableKvsLoadingTask.of(failingCallable, failingMetadata))))
                .getCause()
                .hasMessage("Failed to run");
        assertThat(observer.getMetadata())
                .isEqualTo(ImmutableList.of(succeedingMetadata.getMetadata(), failingMetadata.getMetadata()));
    }

    private static final class TestSpanObserver implements SpanObserver {
        private final List<Map<String, String>> metadata = new ArrayList<>();

        @Override
        public void consume(Span span) {
            metadata.add(span.getMetadata());
        }

        public List<Map<String, String>> getMetadata() {
            return metadata;
        }
    }

    private ImmutableMetadata setMetadata(String s) {
        return ImmutableMetadata.builder()
                .taskName(s)
                .numCells(10)
                .tableRefs(ImmutableSet.of(TableReference.create(Namespace.create("namespace"), "tableRef")))
                .host("host")
                .build();
    }
}
