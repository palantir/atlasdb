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

import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.time.Duration;
import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public final class FilteredCassandraClientInstrumentation implements CassandraClientInstrumentation {
    private final TopListFilteredCounter<String> cellsWrittenCounter;
    private final ExecutorService executorService;

    private FilteredCassandraClientInstrumentation(
            TopListFilteredCounter<String> cellsWrittenCounter, ExecutorService executorService) {
        this.cellsWrittenCounter = cellsWrittenCounter;
        this.executorService = executorService;
    }

    public static CassandraClientInstrumentation create(
            TaggedMetricRegistry registry, ScheduledExecutorService executor) {
        TopListFilteredCounter<String> cellsWritten = TopListFilteredCounter.create(
                5,
                Duration.ofSeconds(5),
                Duration.ofSeconds(15),
                CassandraClientInstrumentationUtils::createCellsWrittenMetricNameForTableTag,
                Comparator.<String>naturalOrder(),
                registry,
                executor);

        return new FilteredCassandraClientInstrumentation(cellsWritten, executor);
    }

    @Override
    public void recordCellsWritten(String tableRef, long cellsWritten) {
        String tableTag = LoggingArgs.safeInternalTableNameOrPlaceholder(tableRef);
        cellsWrittenCounter.inc(tableTag, cellsWritten);
    }

    @Override
    public void close() {
        executorService.shutdownNow();
    }
}
