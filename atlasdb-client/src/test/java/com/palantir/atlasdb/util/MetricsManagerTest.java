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

import java.util.List;
import java.util.ListIterator;

import org.junit.After;
import org.junit.Test;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;

public class MetricsManagerTest {
    private static final Class<List> LIST_CLASS = List.class;
    private static final Class<ListIterator> LIST_ITERATOR_CLASS = ListIterator.class;

    private static final String ERROR_PREFIX = "error";
    private static final String OUT_OF_BOUNDS = "outofbounds";
    private static final String ERROR_OUT_OF_BOUNDS = ERROR_PREFIX + "." + OUT_OF_BOUNDS;
    private static final String OOM = "oom";
    private static final String ERROR_OOM = ERROR_PREFIX + "." + OOM;
    private static final String RUNTIME = "runtime";
    private static final String METER_NAME = "meterName";

    private static final Gauge GAUGE = () -> 1L;
    private static final Meter METER = new Meter();

    private final MetricRegistry registry = new MetricRegistry();
    private final MetricsManager metricsManager = new MetricsManager(registry);

    @Test
    public void registersMetricsByName() {
        metricsManager.registerMetric(LIST_CLASS, ERROR_OOM, GAUGE);

        assertThat(registry.getNames()).containsExactly(MetricRegistry.name(LIST_CLASS, ERROR_OOM));
    }

    @Test
    public void registersMetricsByPrefixAndName() {
        metricsManager.registerMetric(LIST_CLASS, ERROR_PREFIX, OUT_OF_BOUNDS, GAUGE);

        assertThat(registry.getNames()).containsExactly(MetricRegistry.name(LIST_CLASS, ERROR_OUT_OF_BOUNDS));
    }

    @Test
    public void registersMeters() {
        metricsManager.registerOrGetMeter(LIST_CLASS, RUNTIME, METER_NAME);

        assertThat(registry.getMeters().keySet()).containsExactly(
                MetricRegistry.name(LIST_CLASS, RUNTIME, METER_NAME));
    }

    @Test
    public void registersSameMetricNameAcrossClasses() {
        metricsManager.registerMetric(LIST_CLASS, ERROR_OUT_OF_BOUNDS, GAUGE);
        metricsManager.registerMetric(LIST_ITERATOR_CLASS, ERROR_OUT_OF_BOUNDS, GAUGE);

        assertThat(registry.getNames()).containsExactly(
                MetricRegistry.name(LIST_CLASS, ERROR_OUT_OF_BOUNDS),
                MetricRegistry.name(LIST_ITERATOR_CLASS, ERROR_OUT_OF_BOUNDS));
    }

    @Test
    public void deregistersAllMetrics() {
        metricsManager.registerMetric(LIST_CLASS, RUNTIME, GAUGE);
        metricsManager.deregisterMetrics();

        assertThat(registry.getNames()).isEmpty();
    }

    @Test
    public void deregistersMetricsWithSpecificPrefix() {
        metricsManager.registerMetric(LIST_CLASS, ERROR_OUT_OF_BOUNDS, GAUGE);
        metricsManager.registerMetric(LIST_CLASS, ERROR_OOM, GAUGE);
        metricsManager.registerMetric(LIST_CLASS, RUNTIME, GAUGE);

        metricsManager.deregisterMetricsWithPrefix(LIST_CLASS, ERROR_PREFIX);

        assertThat(registry.getNames()).containsExactly(MetricRegistry.name(LIST_CLASS, RUNTIME));
    }

    @Test
    public void deregistersAllMetricsForClassIfDeregisteringWithEmptyPrefix() {
        metricsManager.registerMetric(LIST_CLASS, ERROR_OUT_OF_BOUNDS, GAUGE);
        metricsManager.registerMetric(LIST_CLASS, ERROR_OOM, GAUGE);
        metricsManager.registerMetric(LIST_CLASS, RUNTIME, GAUGE);

        metricsManager.deregisterMetricsWithPrefix(LIST_CLASS, "");
        assertThat(registry.getNames()).isEmpty();
    }

    @Test
    public void doesNotDeregisterMetricsThatAreRegisteredExternally() {
        metricsManager.registerMetric(LIST_CLASS, ERROR_OUT_OF_BOUNDS, GAUGE);
        registry.register(MetricRegistry.name(LIST_CLASS, ERROR_OOM), GAUGE);

        metricsManager.deregisterMetrics();
        assertThat(registry.getNames()).containsExactly(MetricRegistry.name(LIST_CLASS, ERROR_OOM));
    }

    @Test
    public void doesNotDeregisterMetricsThatWereRegisteredExternallyIfDeregisteringWithPrefix() {
        metricsManager.registerMetric(LIST_CLASS, ERROR_OUT_OF_BOUNDS, GAUGE);
        registry.register(MetricRegistry.name(LIST_CLASS, ERROR_OOM), GAUGE);

        metricsManager.deregisterMetricsWithPrefix(LIST_CLASS, ERROR_PREFIX);
        assertThat(registry.getNames()).containsExactly(MetricRegistry.name(LIST_CLASS, ERROR_OOM));
    }

    @Test
    public void doesNotDeregisterMetricsFromOtherClassesEvenIfStringPrefixesMatch() {
        metricsManager.registerMetric(LIST_CLASS, ERROR_OUT_OF_BOUNDS, GAUGE); // java.util.List.error.outofbounds
        metricsManager.registerMetric(LIST_ITERATOR_CLASS, ERROR_OOM, GAUGE); // java.util.ListIterator.error.oom

        metricsManager.deregisterMetricsWithPrefix(LIST_CLASS, "");

        assertThat(registry.getNames()).containsExactly(MetricRegistry.name(LIST_ITERATOR_CLASS, ERROR_OOM));
    }


    @Test
    public void registerOrGetMeterMeterRegistersTheFullyQualifiedClassNameMetric() {
        metricsManager.registerOrGetMeter(LIST_CLASS, ERROR_OUT_OF_BOUNDS);

        assertThat(registry.getNames()).containsExactly("java.util.List.error.outofbounds");
    }

    @After
    public void tearDown() {
        registry.removeMatching(MetricFilter.ALL);
    }
}
