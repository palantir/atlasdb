/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.performance.benchmarks;

import static org.openjdk.jmh.annotations.Level.Invocation;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.naming.Name;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.performance.backend.AtlasDbServicesConnector;
import com.palantir.atlasdb.performance.benchmarks.table.RegeneratingTable;

@State(Scope.Benchmark)
public class KvsDdlBenchmarks {

    // should be larger than the default concurrency (16)
    private static int TABLES_PER_BATCH = 128;
    private Namespace CREATE = Namespace.create("CREATE");
    private Namespace TRUNCATE = Namespace.create("TRUNCATE");
    private Namespace DROP = Namespace.create("DROP");

    private int startOffset = 0;
    private KeyValueService kvs;

    @Setup
    public void setup(AtlasDbServicesConnector connector) {
        kvs = connector.connect().getKeyValueService();
    }

    @Setup(Level.Iteration)
    public void resetState(AtlasDbServicesConnector connector) {
        // drop any tables we've already created
        kvs.dropTables(
                kvs.getAllTableNames().stream()
                        .filter(tableReference -> tableReference.getNamespace().equals(CREATE))
                        .collect(Collectors.toSet()));

        // ensure we have an upcoming batch that truncate and drop can use
        kvs.createTables(
                generateNonOverlappingTableNames();
        )
    }

    /*
 * These tablenames are specifically crafted to partially get around
 * tombstoning issues in Cassandra system tables relating to columnfamily schemas. It's not perfect though;
 * in current Cassandra things like generating a schema hash for purposes of gossiping to others to
 * determine cluster-wide schema agreement does a full table scan of an internal system table
 * that will have to read over tombstoned rows referring to dropped tables.
 */
    private Map<TableReference, byte[]> generateNonOverlappingTableNames(Namespace namespace) {
        return IntStream.rangeClosed(startOffset, startOffset + TABLES_PER_BATCH).boxed().collect(
                Collectors.toMap(
                        n -> TableReference.create(namespace,  "table_" + n),
                        n -> AtlasDbConstants.EMPTY_TABLE_METADATA
                ));
    }

    @Benchmark
    @Threads(1)
    @Measurement(time = 1, timeUnit = TimeUnit.MINUTES)
    public void createManyTables(AtlasDbServicesConnector connector) {
        kvs.createTables(generateNonOverlappingTableNames(CREATE_TEST_PREFIX));
        startOffset += TABLES_PER_BATCH;
    }

    @Benchmark
    @Threads(1)
    @Measurement(time = 1, timeUnit = TimeUnit.MINUTES)
    public void truncateManyTables(AtlasDbServicesConnector connector) {
        KeyValueService kvs = connector.connect().getKeyValueService();
        kvs.truncateTables(generateNonOverlappingTableNames(DROP_AND_TRUNCATE_PREFIX).keySet());
        startOffset += TABLES_PER_BATCH;
    }

    @Benchmark
    @Threads(1)
    @Measurement(time = 1, timeUnit = TimeUnit.MINUTES)
    public void dropManyTables(AtlasDbServicesConnector connector) {
        KeyValueService kvs = connector.connect().getKeyValueService();
        kvs.dropTables(generateNonOverlappingTableNames(DROP_AND_TRUNCATE_PREFIX).keySet());
        startOffset += TABLES_PER_BATCH;
    }
}
