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
package com.palantir.atlasdb.util;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tritium.event.AbstractInvocationEventHandler;
import com.palantir.tritium.event.DefaultInvocationContext;
import com.palantir.tritium.event.InvocationContext;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * A simplified and yet extended version of Tritium's MetricsInvocationEventHandler. This class supports augmenting
 * metric data with tags, possibly as a function of the method's invocation context.
 *
 * Note that this class does not yet offer support for processing of specific metric groups, unlike
 * MetricsInvocationEventHandler.
 */
public class TaggedMetricsInvocationEventHandler extends AbstractInvocationEventHandler<InvocationContext> {
    private static final SafeLogger log = SafeLoggerFactory.get(TaggedMetricsInvocationEventHandler.class);

    private final TaggedMetricRegistry taggedMetricRegistry;
    private final String serviceName;

    private final Optional<Function<InvocationContext, Map<String, String>>> tagFunction;

    @Nullable
    private final Function<Method, Timer> onSuccessTimerMappingFunctionUntagged;

    private final ConcurrentMap<Method, Timer> untaggedTimerCache = new ConcurrentHashMap<>();

    @Nullable
    private final Function<MethodWithExtraTags, Timer> onSuccessTimerMappingFunctionExtraTags;

    private final ConcurrentMap<MethodWithExtraTags, Timer> extraTagsTimerCache = new ConcurrentHashMap<>();

    public TaggedMetricsInvocationEventHandler(TaggedMetricRegistry taggedMetricRegistry, String serviceName) {
        super(InstrumentationUtils.getEnabledSupplier(serviceName));
        this.taggedMetricRegistry = Preconditions.checkNotNull(taggedMetricRegistry, "metricRegistry");
        this.serviceName = Preconditions.checkNotNull(serviceName, "serviceName");
        this.tagFunction = Optional.empty();
        this.onSuccessTimerMappingFunctionUntagged = method -> taggedMetricRegistry.timer(MetricName.builder()
                .safeName(MetricRegistry.name(serviceName, method.getName()))
                .build());
        this.onSuccessTimerMappingFunctionExtraTags = null;
    }

    public TaggedMetricsInvocationEventHandler(
            TaggedMetricRegistry taggedMetricRegistry,
            String serviceName,
            Function<InvocationContext, Map<String, String>> tagFunction) {
        super(InstrumentationUtils.getEnabledSupplier(serviceName));
        this.taggedMetricRegistry = Preconditions.checkNotNull(taggedMetricRegistry, "metricRegistry");
        this.serviceName = Preconditions.checkNotNull(serviceName, "serviceName");
        this.tagFunction = Optional.of(tagFunction);
        this.onSuccessTimerMappingFunctionUntagged = null;
        this.onSuccessTimerMappingFunctionExtraTags = methodAndTags -> taggedMetricRegistry.timer(MetricName.builder()
                .safeName(
                        MetricRegistry.name(serviceName, methodAndTags.method().getName()))
                .putAllSafeTags(methodAndTags.extraTags())
                .build());
    }

    @Override
    public InvocationContext preInvocation(Object instance, Method method, Object[] args) {
        return DefaultInvocationContext.of(instance, method, args);
    }

    @Override
    public void onSuccess(@Nullable InvocationContext context, @Nullable Object result) {
        if (context == null) {
            log.debug("Encountered null metric context likely due to exception in preInvocation");
            return;
        }

        if (result != null
                && ListenableFuture.class.isAssignableFrom(context.getMethod().getReturnType())
                && ListenableFuture.class.isAssignableFrom(result.getClass())) {
            Futures.addCallback(
                    (ListenableFuture<?>) result,
                    new FutureCallback<Object>() {
                        @Override
                        public void onSuccess(Object result) {
                            update(context);
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            TaggedMetricsInvocationEventHandler.this.onFailure(context, t);
                        }
                    },
                    MoreExecutors.directExecutor());
        } else {
            update(context);
        }
    }

    public void update(InvocationContext context) {
        long nanos = System.nanoTime() - context.getStartTimeNanos();
        getSuccessTimer(context).update(nanos, TimeUnit.NANOSECONDS);
    }

    private Timer getSuccessTimer(InvocationContext context) {
        if (tagFunction.isPresent()) {
            Map<String, String> extraTags = tagFunction.get().apply(context);
            return getSuccessTimerExtraTags(ImmutableMethodWithExtraTags.of(context.getMethod(), extraTags));
        } else {
            return getSuccessTimerUntagged(context.getMethod());
        }
    }

    private Timer getSuccessTimerUntagged(Method method) {
        Preconditions.checkNotNull(onSuccessTimerMappingFunctionUntagged);
        return untaggedTimerCache.computeIfAbsent(method, onSuccessTimerMappingFunctionUntagged);
    }

    private Timer getSuccessTimerExtraTags(MethodWithExtraTags methodWithExtraTags) {
        Preconditions.checkNotNull(onSuccessTimerMappingFunctionExtraTags);
        return extraTagsTimerCache.computeIfAbsent(methodWithExtraTags, onSuccessTimerMappingFunctionExtraTags);
    }

    @Value.Immutable
    interface MethodWithExtraTags {
        @Value.Parameter
        Method method();

        @Value.Parameter
        Map<String, String> extraTags();
    }

    @Override
    public void onFailure(@Nullable InvocationContext context, @Nonnull Throwable cause) {
        markGlobalFailure();
        if (context == null) {
            log.debug(
                    "Encountered null metric context likely due to exception in preInvocation: {}",
                    UnsafeArg.of("exception", cause),
                    cause);
            return;
        }

        String failuresMetricName = InstrumentationUtils.getFailuresMetricName(context, serviceName);
        Map<String, String> tags = tagFunction.map(f -> f.apply(context)).orElseGet(ImmutableMap::of);
        taggedMetricRegistry
                .meter(MetricName.builder()
                        .safeName(failuresMetricName)
                        .safeTags(tags)
                        .build())
                .mark();
        taggedMetricRegistry
                .meter(MetricName.builder()
                        .safeName(MetricRegistry.name(
                                failuresMetricName, cause.getClass().getName()))
                        .safeTags(tags)
                        .build())
                .mark();
    }

    private void markGlobalFailure() {
        taggedMetricRegistry
                .meter(InstrumentationUtils.TAGGED_FAILURES_METRIC_NAME)
                .mark();
    }
}
