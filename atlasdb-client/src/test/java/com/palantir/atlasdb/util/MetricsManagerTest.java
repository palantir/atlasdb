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

import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import org.junit.After;
import org.junit.Test;

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

    private final MetricRegistry registry = new MetricRegistry();
    private final TaggedMetricRegistry taggedMetricRegistry = DefaultTaggedMetricRegistry.getDefault();
    private final MetricsManager metricsManager =
            new MetricsManager(registry, taggedMetricRegistry, tableReference -> tableReference
                    .getTablename()
                    .equals("safe"));

    @Test
    public void registersMetricsByName() {
        metricsManager.registerMetric(LIST_CLASS, ERROR_OOM, GAUGE);

        assertThat(registry.getNames()).containsExactly(MetricRegistry.name(LIST_CLASS, ERROR_OOM));
    }

    @Test
    public void registersMeters() {
        metricsManager.registerOrGetMeter(LIST_CLASS, RUNTIME, METER_NAME);

        assertThat(registry.getMeters().keySet()).containsExactly(MetricRegistry.name(LIST_CLASS, RUNTIME, METER_NAME));
    }

    @Test
    public void registersSameMetricNameAcrossClasses() {
        metricsManager.registerMetric(LIST_CLASS, ERROR_OUT_OF_BOUNDS, GAUGE);
        metricsManager.registerMetric(LIST_ITERATOR_CLASS, ERROR_OUT_OF_BOUNDS, GAUGE);

        assertThat(registry.getNames())
                .containsExactly(
                        MetricRegistry.name(LIST_CLASS, ERROR_OUT_OF_BOUNDS),
                        MetricRegistry.name(LIST_ITERATOR_CLASS, ERROR_OUT_OF_BOUNDS));
    }

    @Test
    public void registerOrGetMeterMeterRegistersTheFullyQualifiedClassNameMetric() {
        metricsManager.registerOrGetMeter(LIST_CLASS, ERROR_OUT_OF_BOUNDS);

        assertThat(registry.getNames()).containsExactly("java.util.List.error.outofbounds");
    }

    @Test
    public void getTableNameTagFor_usesSafeTableNames() {
        Map<String, String> tag = metricsManager.getTableNameTagFor(table("safe"));
        assertThat(tag).hasSize(1);
        assertThat(tag).containsKey("tableName");
        assertThat(tag.get("tableName")).isEqualTo("safe");
    }

    @Test
    public void getTableNameTagFor_unsafeTable() {
        Map<String, String> tag = metricsManager.getTableNameTagFor(table("unsafe"));
        assertThat(tag).hasSize(1);
        assertThat(tag).containsKey("tableName");
        assertThat(tag.get("tableName")).isEqualTo("unsafeTable");
    }

    private static TableReference table(String tableName) {
        return TableReference.create(Namespace.create("foo"), tableName);
    }

    @After
    public void tearDown() {
        registry.removeMatching(MetricFilter.ALL);
    }
}
