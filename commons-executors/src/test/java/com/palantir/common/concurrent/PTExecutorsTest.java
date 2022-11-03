/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.common.concurrent;

import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.google.common.collect.MoreCollectors;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Runnables;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.tracing.Observability;
import com.palantir.tracing.Tracer;
import com.palantir.tracing.api.OpenSpan;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanType;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.Test;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName") // Name matches the class we're testing
public class PTExecutorsTest {

    private static String INNER_TRACE_ID = "innerTraceId";
    private static String INNER_SPAN_ID = "innerSpanId";
    private static String INNER_SPAN_PARENT_ID = "innerSpanParentId";

    @Test
    public void testExecutorName_namedThreadFactory() {
        ThreadFactory factory = new NamedThreadFactory("my-prefix");
        assertThat(PTExecutors.getExecutorName(factory)).isEqualTo("my-prefix");
    }

    @Test
    public void testExecutorName_customThreadFactory() {
        ThreadFactory factory = runnable -> {
            Thread thread = Executors.defaultThreadFactory().newThread(runnable);
            thread.setName("foo-3");
            return thread;
        };
        assertThat(PTExecutors.getExecutorName(factory)).isEqualTo("foo");
    }

    @Test
    public void testExecutorName_customThreadFactory_fallback() {
        ThreadFactory factory = runnable -> {
            Thread thread = Executors.defaultThreadFactory().newThread(runnable);
            thread.setName("1");
            return thread;
        };
        assertThat(PTExecutors.getExecutorName(factory)).isEqualTo("PTExecutor");
    }

    @Test
    public void testExecutorThreadLocalState_cachedPoolNospan() {
        Tracer.initTraceWithSpan(Observability.SAMPLE, "RandomID", "outerSpan", SpanType.LOCAL);

        String outerTraceId = Tracer.getTraceId();

        // Because we are doing things within a lamda function, we need to use atomic references outside the lamda
        AtomicReference<String> innerSpanId = new AtomicReference<>("");
        AtomicReference<String> innerSpanParentId = new AtomicReference<>("");
        AtomicReference<String> innerTraceId = new AtomicReference<>("");

        withExecutor(PTExecutors::newCachedThreadPoolWithoutSpan, executor -> {
            // Submit a task of a function that only starts a span within it
            Map<String, String> innerTracInfo = executor.submit(this::spanTask).get();

            // Set our AtomicReference variables for use outside lamda
            innerTraceId.set(innerTracInfo.get(INNER_TRACE_ID));
            innerSpanId.set(innerTracInfo.get(INNER_SPAN_ID));
            innerSpanParentId.set(innerTracInfo.get(INNER_SPAN_PARENT_ID));
        });
        // Close outer span, which lets us collect information about it
        Span outerSpan = Tracer.completeSpan().get();

        assertThat(innerTraceId.get()).isEqualTo(outerTraceId);
        assertThat(outerSpan.getSpanId()).describedAs(null).isNotEqualTo(innerSpanId.get());
        assertThat(innerSpanParentId.get()).isEqualTo(outerSpan.getSpanId());
    }

    @Test
    public void testExecutorThreadLocalState_cachedPool() {
        withExecutor(PTExecutors::newCachedThreadPool, executor -> {
            ExecutorInheritableThreadLocal<String> threadLocal = new ExecutorInheritableThreadLocal<>();
            threadLocal.set("test");
            String result = executor.submit(threadLocal::get).get();
            assertThat(result).isEqualTo("test");
        });
    }

    @Test
    public void testExecutorThreadLocalState_scheduledPool_submit() {
        withExecutor(PTExecutors::newSingleThreadScheduledExecutor, executor -> {
            ExecutorInheritableThreadLocal<String> threadLocal = new ExecutorInheritableThreadLocal<>();
            threadLocal.set("test");
            String result = executor.submit(threadLocal::get).get();
            assertThat(result).isEqualTo("test");
        });
    }

    @Test
    public void testExecutorThreadLocalState_scheduledPool_scheduleOnce() {
        withExecutor(PTExecutors::newSingleThreadScheduledExecutor, executor -> {
            ExecutorInheritableThreadLocal<String> threadLocal = new ExecutorInheritableThreadLocal<>();
            threadLocal.set("test");
            String result = executor.schedule(threadLocal::get, 1, TimeUnit.MILLISECONDS)
                    .get();
            assertThat(result).isEqualTo("test");
        });
    }

    @Test
    public void testExecutorThreadLocalState_scheduledPool_scheduleWithFixedDelay() {
        withExecutor(PTExecutors::newSingleThreadScheduledExecutor, executor -> {
            SettableFuture<String> result = SettableFuture.create();
            ExecutorInheritableThreadLocal<String> threadLocal = new ExecutorInheritableThreadLocal<>();
            threadLocal.set("test");
            ScheduledFuture<?> scheduledFuture =
                    executor.scheduleWithFixedDelay(() -> result.set(threadLocal.get()), 0, 1, TimeUnit.MILLISECONDS);
            String value = result.get();
            scheduledFuture.cancel(true);
            assertThat(value)
                    .describedAs("Executor inheritable state should not be propagated to recurring tasks")
                    .isNull();
        });
    }

    @Test
    public void testExecutorThreadLocalState_scheduledPool_scheduleAtFixedRate() {
        withExecutor(PTExecutors::newSingleThreadScheduledExecutor, executor -> {
            SettableFuture<String> result = SettableFuture.create();
            ExecutorInheritableThreadLocal<String> threadLocal = new ExecutorInheritableThreadLocal<>();
            threadLocal.set("test");
            ScheduledFuture<?> scheduledFuture =
                    executor.scheduleAtFixedRate(() -> result.set(threadLocal.get()), 0, 1, TimeUnit.MILLISECONDS);
            String value = result.get();
            scheduledFuture.cancel(true);
            assertThat(value)
                    .describedAs("Executor inheritable state should not be propagated to recurring tasks")
                    .isNull();
        });
    }

    @Test
    @SuppressWarnings("deprecation") // Testing a component that relies on the singleton TaggedMetricRegistry
    public void testCachedExecutorMetricsRecorded() {
        // Metrics are recorded to the global singleton registry, so we generate a random name to avoid
        // clobbering state from other tests.
        String executorName = UUID.randomUUID().toString();
        TaggedMetricRegistry metrics = SharedTaggedMetricRegistries.getSingleton();
        withExecutor(() -> PTExecutors.newCachedThreadPool(executorName), executor -> {
            executor.execute(Runnables.doNothing());
        });
        Meter submitted = findMetric(
                metrics,
                MetricName.builder()
                        .safeName("executor.submitted")
                        .putSafeTags("executor", executorName)
                        .build(),
                Meter.class);
        assertThat(submitted.getCount()).isOne();
    }

    private static <T extends Metric> T findMetric(TaggedMetricRegistry metrics, MetricName name, Class<T> type) {
        return metrics.getMetrics().entrySet().stream()
                .filter(entry -> {
                    MetricName metricName = entry.getKey();
                    return Objects.equals(name.safeName(), metricName.safeName())
                            && metricName
                                    .safeTags()
                                    .entrySet()
                                    .containsAll(name.safeTags().entrySet());
                })
                .map(Map.Entry::getValue)
                .map(type::cast)
                .collect(MoreCollectors.onlyElement());
    }

    private static <T extends ExecutorService> void withExecutor(Supplier<T> factory, ThrowingConsumer<T> test) {
        T executor = factory.get();
        try {
            test.accept(executor);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            executor.shutdownNow();
            assertThat(MoreExecutors.shutdownAndAwaitTermination(executor, 5, TimeUnit.SECONDS))
                    .describedAs("Executor failed to shutdown within 5 seconds")
                    .isTrue();
        }
    }

    // This method is used only to make a span an return some information about the span
    private Map<String, String> spanTask() {
        OpenSpan innerSpan = Tracer.startSpan("innerSpan");
        Map<String, String> innerTraceInfo = new HashMap<String, String>();
        String innerTraceId = Tracer.getTraceId();
        String innerSpanId = innerSpan.getSpanId();
        String innerSpanParentId = innerSpan.getParentSpanId().get();
        innerTraceInfo.put(INNER_TRACE_ID, innerTraceId);
        innerTraceInfo.put(INNER_SPAN_ID, innerSpanId);
        innerTraceInfo.put(INNER_SPAN_PARENT_ID, innerSpanParentId);
        Tracer.fastCompleteSpan();
        return innerTraceInfo;
    }

    interface ThrowingConsumer<T> {
        void accept(T executor) throws Exception;
    }
}
