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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public class SweepMetricImpl implements SweepMetric {
    private final String name;
    private final TaggedMetricRegistry metricRegistry;
    private final UpdateEvent updateEvent;
    private final boolean tagWithTableName;
    private final SweepMetricAdapter<?> metricAdapter;

    SweepMetricImpl(SweepMetricConfig config) {
        this.name = config.name();
        this.metricRegistry = config.taggedMetricRegistry();
        this.updateEvent = config.updateEvent();
        this.tagWithTableName = config.tagWithTableName();
        this.metricAdapter = config.metricAdapter();
    }

    SweepMetricImpl(String namePrefix, TaggedMetricRegistry taggedMetricRegistry, UpdateEvent updateEvent,
            boolean tagWithTableName, SweepMetricAdapter<?> sweepMetricAdapter) {
        this.name = namePrefix + sweepMetricAdapter.getNameSuffix();
        this.metricRegistry = taggedMetricRegistry;
        this.updateEvent = updateEvent;
        this.tagWithTableName = tagWithTableName;
        this.metricAdapter = sweepMetricAdapter;
    }

    @Override
    public void update(long value, TableReference tableRef, UpdateEvent eventInstance) {
        if (updateEvent.equals(eventInstance)) {
            metricAdapter.updateMetric(
                    metricRegistry, getTaggedMetricName(name, updateEvent, tableRef, tagWithTableName), value);
        }
    }

    @VisibleForTesting
    static MetricName getTaggedMetricName(String name, UpdateEvent updateEvent, TableReference tableRef,
            boolean taggedWithTableName) {
        return MetricName.builder()
                .safeName(MetricRegistry.name(name))
                .safeTags(constructTags(updateEvent, taggedWithTableName ? Optional.of(tableRef) : Optional.empty()))
                .build();
    }

    private static Map<String, String> constructTags(UpdateEvent updateEvent, Optional<TableReference> maybeTableRef) {
        Map<String, String> tags = new HashMap<>(2);
        tags.put(updateEvent.getTag(), updateEvent.getLabel());
        if (maybeTableRef.isPresent()) {
            TableReference safeTableRef = LoggingArgs.safeTableOrPlaceholder(maybeTableRef.get());
            tags.put("tableRef", safeTableRef.toString());
        }
        return tags;
    }
}
