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
import com.palantir.tritium.event.InstrumentationProperties;
import com.palantir.tritium.event.InvocationContext;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

final class InstrumentationUtils {
    static final String FAILURES_METRIC_NAME = "failures";

    private InstrumentationUtils() {
        // utility
    }

    static BooleanSupplier getEnabledSupplier(final String serviceName) {
        return InstrumentationProperties.getSystemPropertySupplier(serviceName);
    }

    static Timer createNewTimer() {
        return new Timer(new SlidingTimeWindowArrayReservoir(35, TimeUnit.SECONDS));
    }

    static String getBaseMetricName(Method method, String serviceName) {
        return MetricRegistry.name(serviceName, method.getName());
    }

    static String getBaseMetricName(InvocationContext context, String serviceName) {
        return getBaseMetricName(context.getMethod(), serviceName);
    }

    static String getFailuresMetricName(InvocationContext context, String serviceName) {
        return MetricRegistry.name(getBaseMetricName(context, serviceName), FAILURES_METRIC_NAME);
    }
}
