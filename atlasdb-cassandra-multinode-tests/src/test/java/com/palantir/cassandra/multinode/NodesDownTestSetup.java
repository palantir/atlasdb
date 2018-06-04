/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 * <p>
 * Licensed under the BSD-3 License (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.palantir.cassandra.multinode;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.ClassRule;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.containers.ThreeNodeCassandraCluster;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolImpl;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.docker.compose.connection.DockerPort;

public abstract class NodesDownTestSetup {

    private static final int CASSANDRA_THRIFT_PORT = 9160;

    static final TableReference TEST_TABLE = TableReference.createWithEmptyNamespace("test_table");
    static final TableReference TEST_TABLE_TO_DROP = TableReference.createWithEmptyNamespace("test_table_to_drop");
    static final TableReference TEST_TABLE_TO_DROP_2 = TableReference.createWithEmptyNamespace("test_table_to_drop_2");

    static final byte[] FIRST_ROW = PtBytes.toBytes("row1");
    static final byte[] SECOND_ROW = PtBytes.toBytes("row2");
    static final byte[] FIRST_COLUMN = PtBytes.toBytes("col1");
    static final byte[] SECOND_COLUMN = PtBytes.toBytes("col2");
    static final Cell CELL_1_1 = Cell.create(FIRST_ROW, FIRST_COLUMN);
    static final Cell CELL_1_2 = Cell.create(FIRST_ROW, SECOND_COLUMN);
    static final Cell CELL_2_1 = Cell.create(SECOND_ROW, FIRST_COLUMN);
    static final Cell CELL_2_2 = Cell.create(SECOND_ROW, SECOND_COLUMN);
    static final Cell CELL_3_1 = Cell.create(PtBytes.toBytes("row3"), FIRST_COLUMN);
    static final Cell CELL_4_1 = Cell.create(PtBytes.toBytes("row4"), FIRST_COLUMN);

    static final byte[] DEFAULT_CONTENTS = PtBytes.toBytes("default_value");
    static final long DEFAULT_TIMESTAMP = 2L;
    static final long OLD_TIMESTAMP = 1L;
    static final Value DEFAULT_VALUE = Value.create(DEFAULT_CONTENTS, DEFAULT_TIMESTAMP);
    static final ImmutableCassandraKeyValueServiceConfig CONFIG = ImmutableCassandraKeyValueServiceConfig
            .copyOf(ThreeNodeCassandraCluster.KVS_CONFIG)
            .withSchemaMutationTimeoutMillis(3_000);

    static CassandraKeyValueService kvs;

    @ClassRule
    public static final Containers CONTAINERS = new Containers(NodesDownTestSetup.class)
            .with(new ThreeNodeCassandraCluster());

    public static void initializeKvsAndDegradeCluster(List<String> nodesToKill) {
        setupTestTable();
        kvs = createCassandraKvs();
        degradeCassandraCluster(nodesToKill);
    }

    @AfterClass
    public static void closeKvs() throws IOException, InterruptedException {
        kvs.close();
    }

    private static void setupTestTable() {
        CassandraKeyValueService setupDb = createCassandraKvs();
        setupDb.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        setupDb.put(TEST_TABLE, ImmutableMap.of(CELL_1_1, PtBytes.toBytes("old_value")), OLD_TIMESTAMP);
        setupDb.put(TEST_TABLE, ImmutableMap.of(CELL_1_1, DEFAULT_CONTENTS), DEFAULT_TIMESTAMP);
        setupDb.put(TEST_TABLE, ImmutableMap.of(CELL_1_2, DEFAULT_CONTENTS), DEFAULT_TIMESTAMP);
        setupDb.put(TEST_TABLE, ImmutableMap.of(CELL_2_1, DEFAULT_CONTENTS), DEFAULT_TIMESTAMP);

        setupDb.createTable(TEST_TABLE_TO_DROP, AtlasDbConstants.GENERIC_TABLE_METADATA);
        setupDb.createTable(TEST_TABLE_TO_DROP_2, AtlasDbConstants.GENERIC_TABLE_METADATA);

        setupDb.close();
    }

    protected static CassandraKeyValueService createCassandraKvs() {
        return CassandraKeyValueServiceImpl.create(
                MetricsManagers.createForTests(),
                CONFIG, ThreeNodeCassandraCluster.LEADER_CONFIG);
    }

    private static void degradeCassandraCluster(List<String> nodesToKill) {
        nodesToKill.forEach((containerName) -> {
            try {
                killCassandraContainer(containerName);
            } catch (IOException | InterruptedException e) {
                Throwables.propagate(e);
            }
        });

        // startup checks aren't guaranteed to pass immediately after killing the node, so we wait until
        // they do. unclear if this is an AtlasDB product problem. see #1154
        if (nodesToKill.size() < 2) {
            waitUntilStartupChecksPass();
        }
    }

    private static void killCassandraContainer(String containerName) throws IOException, InterruptedException {
        CONTAINERS.getContainer(containerName).kill();
        DockerPort containerPort = new DockerPort(containerName, CASSANDRA_THRIFT_PORT, CASSANDRA_THRIFT_PORT);
        Awaitility.waitAtMost(10, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS).until(
                () -> !containerPort.isListeningNow());
    }

    private static void waitUntilStartupChecksPass() {
        Awaitility.await()
                .atMost(180, TimeUnit.SECONDS)
                .until(NodesDownTestSetup::startupChecksPass);
    }

    private static boolean startupChecksPass() {
        try {
            // startup checks are done implicitly in the constructor
            CassandraClientPoolImpl.create(
                    MetricsManagers.createForTests(),
                    ThreeNodeCassandraCluster.KVS_CONFIG);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    protected static void verifyValue(Cell cell, Value value) {
        Map<Cell, Value> result = kvs.get(TEST_TABLE, ImmutableMap.of(cell, Long.MAX_VALUE));
        assertThat(value).isEqualTo(result.get(cell));
    }

    protected static boolean tableExists(TableReference tableReference) {
        return NodesDownTestSetup.kvs.getAllTableNames().contains(tableReference);
    }
}
