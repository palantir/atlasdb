/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public final class UnfilteredCassandraClientInstrumentation implements CassandraClientInstrumentation {
    private final TaggedMetricRegistry registry;

    public UnfilteredCassandraClientInstrumentation(TaggedMetricRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void recordCellsWritten(String tableRef, long cellsWritten) {
        registry.counter(MetricName.builder()
                        .safeName(MetricRegistry.name(CassandraClient.class, "cellsWritten"))
                        .safeTags(ImmutableMap.of("tableRef", LoggingArgs.safeInternalTableNameOrPlaceholder(tableRef)))
                        .build())
                .inc(cellsWritten);
    }
}
