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

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.tritium.event.AbstractInvocationEventHandler;
import com.palantir.tritium.event.DefaultInvocationContext;
import com.palantir.tritium.event.InvocationContext;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

/**
 * A simplified and yet extended version of Tritium's MetricsInvocationEventHandler. This class supports augmenting
 * metric data with a constant set of tags.
 *
 * Note that this class does not yet offer support for processing of specific metric groups, unlike
 * MetricsInvocationEventHandler.
 */
public class TaggedMetricsInvocationEventHandler extends AbstractInvocationEventHandler<InvocationContext> {
    private static final Logger logger = LoggerFactory.getLogger(TaggedMetricsInvocationEventHandler.class);

    private final TaggedMetricRegistry taggedMetricRegistry;
    private final String serviceName;

    private final Map<String, String> tags;

    private final Meter globalFailureMeter;
    private final ConcurrentMap<Method, Timer> successTimerCache = new ConcurrentHashMap<>();

    public TaggedMetricsInvocationEventHandler(
            TaggedMetricRegistry taggedMetricRegistry,
            String serviceName,
            Map<String, String> tags) {
        super(InstrumentationUtils.getEnabledSupplier(serviceName));
        this.taggedMetricRegistry = Preconditions.checkNotNull(taggedMetricRegistry, "metricRegistry");
        this.serviceName = Preconditions.checkNotNull(serviceName, "serviceName");
        this.tags = tags;

        this.globalFailureMeter = initializeGlobalFailureMeter();
    }

    @Override
    public InvocationContext preInvocation(Object instance, Method method, Object[] args) {
        return DefaultInvocationContext.of(instance, method, args);
    }

    @Override
    public void onSuccess(@Nullable InvocationContext context, @Nullable Object result) {
        if (context == null) {
            logger.debug("Encountered null metric context likely due to exception in preInvocation");
            return;
        }

        long nanos = System.nanoTime() - context.getStartTimeNanos();
        successTimerCache.computeIfAbsent(context.getMethod(), this::getTimer).update(nanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public void onFailure(@Nullable InvocationContext context, @Nonnull Throwable cause) {
        globalFailureMeter.mark();
        if (context == null) {
            logger.debug("Encountered null metric context likely due to exception in preInvocation: {}",
                    UnsafeArg.of("exception", cause),
                    cause);
            return;
        }

        String failuresMetricName = InstrumentationUtils.getFailuresMetricName(context, serviceName);
        taggedMetricRegistry.meter(MetricName.builder().safeName(failuresMetricName).build()).mark();
        taggedMetricRegistry.meter(MetricName.builder().safeName(
                MetricRegistry.name(failuresMetricName, cause.getClass().getName())).build())
                .mark();

    }

    private Timer getTimer(Method method) {
        MetricName finalMetricName = MetricName.builder()
                .safeName(MetricRegistry.name(serviceName, method.getName()))
                .putAllSafeTags(tags)
                .build();
        return taggedMetricRegistry.timer(finalMetricName);
    }

    private Meter initializeGlobalFailureMeter() {
        return taggedMetricRegistry.meter(MetricName.builder()
                .safeName(InstrumentationUtils.FAILURES_METRIC_NAME)
                .build());
    }
}
