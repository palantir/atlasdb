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

import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.junit.After;
import org.junit.Test;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

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
    private final MetricsManager metricsManager = new MetricsManager(registry, taggedMetricRegistry,
            new HashSet<>(), tableReference -> tableReference.getTablename().equals("safe"));

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
    public void registersSingleTaggedGauge() {
        ImmutableMap<String, String> oneTag = ImmutableMap.of("tag1", "value");
        metricsManager.registerMetric(LIST_CLASS, METER_NAME, GAUGE, oneTag);

        assertThat(taggedMetricRegistry.getMetrics().keySet())
                .contains(MetricName.builder()
                        .safeName(MetricRegistry.name(LIST_CLASS, METER_NAME))
                        .safeTags(oneTag)
                        .build());
    }

    @Test
    public void registersMultipleTaggedGauge() {
        ImmutableMap<String, String> oneTag = ImmutableMap.of("tag1", "value", "tag2", "value2");
        metricsManager.registerMetric(LIST_CLASS, METER_NAME, GAUGE, oneTag);

        assertThat(taggedMetricRegistry.getMetrics().keySet())
                .contains(MetricName.builder()
                        .safeName(MetricRegistry.name(LIST_CLASS, METER_NAME))
                        .safeTags(oneTag)
                        .build());
    }

    @Test
    public void registersSameGaugeMultipleTimesWithDifferentTags() {
        ImmutableMap<String, String> gauge1Tags = ImmutableMap.of("tag1", "value");
        ImmutableMap<String, String> gauge2Tags = ImmutableMap.of("tag2", "value2");
        metricsManager.registerMetric(LIST_CLASS, METER_NAME, GAUGE, gauge1Tags);
        metricsManager.registerMetric(LIST_CLASS, METER_NAME, GAUGE, gauge2Tags);

        assertThat(taggedMetricRegistry.getMetrics().keySet())
                .contains(MetricName.builder().safeName(MetricRegistry.name(LIST_CLASS, METER_NAME)).safeTags(gauge1Tags).build(),
                        MetricName.builder().safeName(MetricRegistry.name(LIST_CLASS, METER_NAME)).safeTags(gauge2Tags).build());
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

    @Test
    public void getTableNameTagFor_usesSafeTableNames() {
        Map<String, String> tag = metricsManager.getTableNameTagFor(table("safe"));
        assertThat(tag.size()).isEqualTo(1);
        assertThat(tag).containsKey("tableName");
        assertThat(tag.get("tableName")).isEqualTo("safe");
    }

    @Test
    public void getTableNameTagFor_obfuscatesUnsafeTables() {
        Map<String, String> tag = metricsManager.getTableNameTagFor(table("unsafe"));
        assertThat(tag.size()).isEqualTo(1);
        assertThat(tag).containsKey("tableName");
        assertThat(tag.get("tableName")).isEqualTo("unsafeTable_629e3fc948fb5ca5");
    }

    private TableReference table(String tableName) {
        return TableReference.create(Namespace.create("foo"), tableName);
    }

    @After
    public void tearDown() {
        registry.removeMatching(MetricFilter.ALL);
    }
}
