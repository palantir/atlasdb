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

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.Timer;
import com.palantir.tritium.event.InstrumentationProperties;
import com.palantir.tritium.event.InvocationContext;

public final class MetricsInvocationEventHandlerUtils {
    public static final String FAILURES_METRIC_NAME = "failures";

    private MetricsInvocationEventHandlerUtils() {
        // utility
    }

    public static BooleanSupplier getEnabledSupplier(final String serviceName) {
        return InstrumentationProperties.getSystemPropertySupplier(serviceName);
    }

    public static Timer createNewTimer() {
        return new Timer(new SlidingTimeWindowArrayReservoir(35, TimeUnit.SECONDS));
    }

    public static String getBaseMetricName(InvocationContext context, String serviceName) {
        return MetricRegistry.name(serviceName, context.getMethod().getName());
    }

    public static String getFailuresMetricName(InvocationContext context, String serviceName) {
        return MetricRegistry.name(getBaseMetricName(context, serviceName), FAILURES_METRIC_NAME);
    }
}
