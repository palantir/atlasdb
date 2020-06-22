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

package com.palantir.atlasdb.util;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.Timer;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.tritium.event.AbstractInvocationEventHandler;
import com.palantir.tritium.event.DefaultInvocationContext;
import com.palantir.tritium.event.InvocationContext;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A modified version of {@link com.palantir.tritium.event.metrics.MetricsInvocationEventHandler} that uses a
 * {@link SlidingTimeWindowArrayReservoir} for the timer's reservoir.
 */
public final class SlidingWindowMetricsInvocationHandler extends AbstractInvocationEventHandler<InvocationContext> {
    private static final Logger logger = LoggerFactory.getLogger(SlidingWindowMetricsInvocationHandler.class);

    private final MetricRegistry metricRegistry;
    private final Map<Method, Timer> timers = new ConcurrentHashMap<>();
    private final String serviceName;

    public SlidingWindowMetricsInvocationHandler(
            MetricRegistry metricRegistry, String serviceName) {
        super(InstrumentationUtils.getEnabledSupplier(serviceName));
        this.metricRegistry = Preconditions.checkNotNull(metricRegistry, "metricRegistry");
        this.serviceName = Preconditions.checkNotNull(serviceName, "serviceName");
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
        Method method = context.getMethod();
        Timer timer = timers.computeIfAbsent(method, this::getTimer);
        timer.update(nanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public void onFailure(@Nullable InvocationContext context, @Nonnull Throwable cause) {
        markGlobalFailure();
        if (context == null) {
            logger.debug("Encountered null metric context likely due to exception in preInvocation: {}",
                    UnsafeArg.of("cause", cause),
                    cause);
            return;
        }

        String failuresMetricName = InstrumentationUtils.getFailuresMetricName(context, serviceName);
        metricRegistry.meter(failuresMetricName).mark();
        metricRegistry.meter(MetricRegistry.name(failuresMetricName, cause.getClass().getName())).mark();
    }

    private Timer getTimer(Method method) {
        return metricRegistry.timer(
                InstrumentationUtils.getBaseMetricName(method, serviceName),
                InstrumentationUtils::createNewTimer);
    }

    private void markGlobalFailure() {
        metricRegistry.meter(InstrumentationUtils.FAILURES_METRIC_NAME).mark();
    }
}
