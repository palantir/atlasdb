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
package com.palantir.atlasdb.keyvalue.cassandra;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

@SuppressWarnings({"all"}) // thrift variable names.
public class InstrumentedCassandraClient implements AutoDelegate_CassandraClient {
    private final CassandraClient delegate;
    private final TaggedMetricRegistry taggedMetricRegistry;

    public InstrumentedCassandraClient(CassandraClient client, TaggedMetricRegistry taggedMetricRegistry) {
        this.delegate = client;
        this.taggedMetricRegistry = taggedMetricRegistry;
    }

    @Override
    public CassandraClient delegate() {
        return delegate;
    }

    @Override
    public void batch_mutate(
            String kvsMethodName,
            Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        delegate.batch_mutate(kvsMethodName, mutation_map, consistency_level);

        Map<String, Long> tablesToCells = Maps.newHashMapWithExpectedSize(mutation_map.size());

        mutation_map.values().forEach(tableToCellsMap -> {
            tableToCellsMap.forEach((table, cells) -> {
                Long numberOfCells = tablesToCells.getOrDefault(table, 0L);
                tablesToCells.put(table, numberOfCells + cells.size());
            });
        });

        tablesToCells.forEach(this::updateCellsWrittenForTable);
    }

    private void updateCellsWrittenForTable(String table, Long numberOfCells) {
        taggedMetricRegistry
                .counter(MetricName.builder()
                        .safeName(MetricRegistry.name(CassandraClient.class, "cellsWritten"))
                        .safeTags(ImmutableMap.of("tableRef", LoggingArgs.safeInternalTableNameOrPlaceholder(table)))
                        .build())
                .inc(numberOfCells);
    }
}
