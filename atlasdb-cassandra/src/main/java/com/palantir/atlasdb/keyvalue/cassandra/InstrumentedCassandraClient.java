/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.processors.AutoDelegate;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

@SuppressWarnings({"all"}) // thrift variable names.
@AutoDelegate(typeToExtend = CassandraClient.class)
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
    public void batch_mutate(String kvsMethodName,
            Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map,
            ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        mutation_map.forEach((key, tableToMutations) -> {
            tableToMutations.forEach((table, mutations) -> {
                getCellsWrittenMeterForTable(table).mark(mutations.size());
            });
        });
    }

    private Meter getCellsWrittenMeterForTable(String table) {
        return taggedMetricRegistry.meter(
                MetricName.builder()
                .safeName(MetricRegistry.name(CassandraClient.class, "cellsWritten"))
                .safeTags(ImmutableMap.of("tableRef", LoggingArgs.safeInternalTableNameOrPlaceholder(table)))
                .build());
    }
}
