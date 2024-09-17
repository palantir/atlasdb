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

import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public final class UnfilteredCassandraClientMetrics implements CassandraClientMetrics {
    private final TaggedMetricRegistry registry;

    public UnfilteredCassandraClientMetrics(TaggedMetricRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void recordCellsWritten(String tableRef, long cellsWritten) {
        registry.counter(CassandraClientInstrumentationUtils.createCellsWrittenMetricNameForTableTag(tableRef))
                .inc(cellsWritten);
    }
}
