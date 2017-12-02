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

package com.palantir.atlasdb.sweep.metrics;

import java.util.Map;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.tritium.metrics.registry.MetricName;

public abstract class SweepTaggedMetric {
    final String name;
    final Boolean taggedWithTableName;

    SweepTaggedMetric(String name, Boolean taggedWithTableName) {
        this.name = name;
        this.taggedWithTableName = taggedWithTableName;
    }

    public abstract MetricType getMetricType();

    MetricName getMetricName(TableReference tableRef, UpdateEvent updateEvent) {
        return getTaggedMetricName(name, getMetricType(), updateEvent, tableRef, taggedWithTableName);
    }

    @VisibleForTesting
    static MetricName getTaggedMetricName(String name, MetricType metricType, UpdateEvent updateEvent,
            TableReference tableRef, Boolean taggedWithTableName) {
        return MetricName.builder()
                .safeName(MetricRegistry.name(SweepMetricsManager.class, name + metricType.getLabel()))
                .safeTags(constructTags(metricType, updateEvent, tableRef, taggedWithTableName))
                .build();
    }

    private static Map<String, String> constructTags(MetricType metricType, UpdateEvent updateEvent,
            TableReference tableRef, Boolean taggedWithTableName) {
        ImmutableMap.Builder<String, String> tagsBuilder = ImmutableMap.builder();
        tagsBuilder.put(metricType.getTag(), metricType.getLabel());
        tagsBuilder.put(updateEvent.getTag(), updateEvent.getLabel());
        if (taggedWithTableName) {
            TableReference safeTableRef = LoggingArgs.safeTableOrPlaceholder(tableRef);
            tagsBuilder.put("tableRef", safeTableRef.toString());
        }
        return tagsBuilder.build();
    }
}
