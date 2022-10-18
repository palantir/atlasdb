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
import static org.junit.Assert.assertNotEquals;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.google.common.collect.MoreCollectors;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Runnables;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.tracing.CloseableTracer;
import com.palantir.tracing.Observability;
import com.palantir.tracing.Tracer;
import com.palantir.tracing.api.OpenSpan;
import com.palantir.tracing.api.Span;
import com.palantir.tracing.api.SpanType;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
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
        Tracer.initTraceWithSpan(Observability.SAMPLE, "RandomID", "outerThreadTest", SpanType.LOCAL);

        String outerTraceId = Tracer.getTraceId();
        AtomicReference<String> innerSpanId = new AtomicReference<>("");
        AtomicReference<String> innerSpanParentId = new AtomicReference<>("");
        AtomicReference<String> innerTraceId = new AtomicReference<>("");

        withExecutor(PTExecutors::newCachedThreadPoolWithoutSpan, executor -> {
            OpenSpan innerSpan = Tracer.startSpan("innerThreadTest");

            innerTraceId.set(Tracer.getTraceId());
            innerSpanId.set(innerSpan.getSpanId());
            innerSpanParentId.set(innerSpan.getParentSpanId().get());

            // Close innerSpan
            Tracer.fastCompleteSpan();
        });
        Span outerSpan = Tracer.completeSpan().get();

        // Proves that both inner and outer spans are part of the same trace.
        assertThat(innerTraceId.get()).isEqualTo(outerTraceId);

        // Proves that the inner span and outer span are not the same
        assertNotEquals(null, innerSpanId.get(), outerSpan.getSpanId());

        // Proves that the inner span is a child of the outer span
        assertThat(innerSpanParentId.get()).isEqualTo(outerSpan.getSpanId());
    }

    @Test
    public void testExecutorThreadLocalState_cachedPoolNospanNotLocal() {
        CloseableTracer outerTrace = CloseableTracer.startSpan("outerThreadTest");
        String outerTraceId = Tracer.getTraceId();
        System.out.println("name");
        System.out.println(Thread.currentThread().getName());

        withExecutor(PTExecutors::newCachedThreadPool, executor -> {
            ExecutorInheritableThreadLocal<String> threadLocal = new ExecutorInheritableThreadLocal<>();

            threadLocal.set("test");
            System.out.println("name");
            System.out.println(Thread.currentThread().getName());
            // System.out.print(threadLocal.get());

            CloseableTracer innerTrace = CloseableTracer.startSpan("innerThreadTest");
            String innerTraceId = Tracer.getTraceId();

            String result = executor.submit(threadLocal::get).get();

            System.out.println(result);
            assertThat(result).isNull();

            innerTrace.close();
            assertThat(outerTraceId).isEqualTo(innerTraceId);
        });
        outerTrace.close();
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
            try (CloseableTracer tracer = CloseableTracer.startSpan("innerThreadTest")) {
                String result = executor.submit(threadLocal::get).get();
                assertThat(result).isEqualTo("innerThreadTest");
            }
            // assertThat(result).isEqualTo("test");
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

    interface ThrowingConsumer<T> {
        void accept(T executor) throws Exception;
    }
}
