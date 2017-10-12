/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.Test;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;

public class MetricsManagerTest {
    private static final Class<Thread> THREAD_CLASS = Thread.class;
    private static final Class<ThreadLocal> THREAD_LOCAL_CLASS = ThreadLocal.class;

    private static final String ERROR_PREFIX = "errors";
    private static final String ERROR_DEATH_SUFFIX = "death";
    private static final String ERROR_OVERFLOW = ERROR_PREFIX + "." + ERROR_DEATH_SUFFIX;
    private static final String ERROR_BLOCK_INDEFINITELY_SUFFIX = "block.indefinitely";
    private static final String ERROR_FORMAT = ERROR_PREFIX + "." + ERROR_BLOCK_INDEFINITELY_SUFFIX;
    private static final String UTILIZATION = "utilization";
    private static final String RUNTIME = "runtime";
    public static final String METER_NAME = "meterName";

    private final MetricsManager metricsManager = new MetricsManager();
    private final MetricRegistry registry = metricsManager.getRegistry();

    @Test
    public void canRegisterMetricsByName() {
        metricsManager.registerMetric(THREAD_CLASS, UTILIZATION, () -> 5L);

        assertThat(registry.getNames()).containsExactly(convertToFqnAndDotDelimit(THREAD_CLASS, UTILIZATION));
    }

    @Test
    public void canRegisterMetricsByPrefixAndName() {
        metricsManager.registerMetric(THREAD_CLASS, ERROR_PREFIX, ERROR_DEATH_SUFFIX, () -> 1L);

        assertThat(registry.getNames()).containsExactly(convertToFqnAndDotDelimit(THREAD_CLASS, ERROR_OVERFLOW));
    }

    @Test
    public void canRegisterMeters() {
        metricsManager.registerMeter(THREAD_CLASS, RUNTIME, METER_NAME);

        assertThat(registry.getMeters().keySet()).containsExactly(
                convertToFqnAndDotDelimit(THREAD_CLASS, RUNTIME, METER_NAME));
    }

    @After
    public void tearDown() {
        registry.removeMatching(MetricFilter.ALL);
    }

    private static <T> String convertToFqnAndDotDelimit(Class<T> clazz, String... components) {
        return String.join(".", Lists.asList(clazz.getName(), components));
    }
}
