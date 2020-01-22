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
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.tritium.event.AbstractInvocationEventHandler;
import com.palantir.tritium.event.DefaultInvocationContext;
import com.palantir.tritium.event.InvocationContext;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simplified and yet extended version of Tritium's MetricsInvocationEventHandler. This class supports augmenting
 * metric data with tags, possibly as a function of the method's invocation context.
 *
 * Note that this class does not yet offer support for processing of specific metric groups, unlike
 * MetricsInvocationEventHandler.
 */
public class TaggedMetricsInvocationEventHandler extends AbstractInvocationEventHandler<InvocationContext> {
    private static final Logger logger = LoggerFactory.getLogger(TaggedMetricsInvocationEventHandler.class);

    private final TaggedMetricRegistry taggedMetricRegistry;
    private final String serviceName;

    private final Function<InvocationContext, Map<String, String>> tagFunction;

    public TaggedMetricsInvocationEventHandler(
            TaggedMetricRegistry taggedMetricRegistry,
            String serviceName,
            Function<InvocationContext, Map<String, String>> tagFunction) {
        super(InstrumentationUtils.getEnabledSupplier(serviceName));
        this.taggedMetricRegistry = Preconditions.checkNotNull(taggedMetricRegistry, "metricRegistry");
        this.serviceName = Preconditions.checkNotNull(serviceName, "serviceName");
        this.tagFunction = tagFunction;
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
        MetricName finalMetricName = MetricName.builder()
                .safeName(MetricRegistry.name(serviceName, context.getMethod().getName()))
                .putAllSafeTags(tagFunction.apply(context))
                .build();
        taggedMetricRegistry.timer(finalMetricName).update(nanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public void onFailure(@Nullable InvocationContext context, @Nonnull Throwable cause) {
        markGlobalFailure();
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

    private void markGlobalFailure() {
        taggedMetricRegistry.meter(MetricName.builder()
                .safeName(InstrumentationUtils.FAILURES_METRIC_NAME)
                .build())
                .mark();
    }
}
