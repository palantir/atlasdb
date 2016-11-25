/**
 * Copyright 2016 Palantir Technologies
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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.google.common.collect.ImmutableMap;
import com.jayway.awaitility.Awaitility;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.containers.ThreeNodeCassandraCluster;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.docker.compose.connection.Container;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        OneNodeDownGetTest.class,
        OneNodeDownPutTest.class,
        OneNodeDownMetadataTest.class,
        OneNodeDownDeleteTest.class,
        OneNodeDownTableManipulationTest.class
        })
public abstract class OneNodeDownTestSuite {

    private static final String CASSANDRA_NODE_TO_KILL = ThreeNodeCassandraCluster.FIRST_CASSANDRA_CONTAINER_NAME;

    public static final TableReference TEST_TABLE = TableReference.createWithEmptyNamespace("test_table");

    public static final byte[] FIRST_ROW = "row1".getBytes();
    public static final byte[] SECOND_ROW = "row2".getBytes();
    public static final byte[] FIRST_COLUMN = "col1".getBytes();
    public static final byte[] SECOND_COLUMN = "col2".getBytes();
    public static final Cell CELL_1_1 = Cell.create(FIRST_ROW, FIRST_COLUMN);
    public static final Cell CELL_1_2 = Cell.create(FIRST_ROW, SECOND_COLUMN);
    public static final Cell CELL_2_1 = Cell.create(SECOND_ROW, FIRST_COLUMN);
    public static final Cell CELL_2_2 = Cell.create(SECOND_ROW, SECOND_COLUMN);
    public static final Cell CELL_3_1 = Cell.create("row3".getBytes(), FIRST_COLUMN);
    public static final Cell CELL_3_2 = Cell.create("row3".getBytes(), SECOND_COLUMN);

    public static final byte[] DEFAULT_VALUE = "default_value".getBytes();
    public static final long DEFAULT_TIMESTAMP = 2L;

    public static CassandraKeyValueService db;

    @ClassRule
    public static final Containers CONTAINERS = new Containers(OneNodeDownTestSuite.class)
            .with(new ThreeNodeCassandraCluster());

    @BeforeClass
    public static void initializeKvsAndDegradeCluster() throws IOException, InterruptedException {
        setupTestTable();
        degradeCassandraCluster();
        db = createCassandraKvs();
    }

    @AfterClass
    public static void bringClusterBack() throws IOException, InterruptedException {
        db.close();
    }

    private static void setupTestTable() {
        db = createCassandraKvs();

        db.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        db.put(TEST_TABLE, ImmutableMap.of(CELL_1_1, "old_value".getBytes()), 1);
        db.put(TEST_TABLE, ImmutableMap.of(CELL_1_1, DEFAULT_VALUE), DEFAULT_TIMESTAMP);
        db.put(TEST_TABLE, ImmutableMap.of(CELL_1_2, DEFAULT_VALUE), DEFAULT_TIMESTAMP);
        db.put(TEST_TABLE, ImmutableMap.of(CELL_2_1, DEFAULT_VALUE), DEFAULT_TIMESTAMP);

        db.close();
    }

    protected static CassandraKeyValueService createCassandraKvs() {
        return CassandraKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(ThreeNodeCassandraCluster.KVS_CONFIG),
                ThreeNodeCassandraCluster.LEADER_CONFIG);
    }

    private static void degradeCassandraCluster() throws IOException, InterruptedException {
        killFirstCassandraNode();

        // startup checks aren't guaranteed to pass immediately after killing the node, so we wait until
        // they do. unclear if this is an AtlasDB product problem. see #1154
        waitUntilStartupChecksPass();
    }

    private static void killFirstCassandraNode() throws IOException, InterruptedException {
        Container container = CONTAINERS.getContainer(CASSANDRA_NODE_TO_KILL);
        container.kill();
    }


    private static void waitUntilStartupChecksPass() {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .until(OneNodeDownTestSuite::startupChecksPass);
    }

    private static boolean startupChecksPass() {
        CassandraKeyValueServiceConfigManager manager = CassandraKeyValueServiceConfigManager.createSimpleManager(
                ThreeNodeCassandraCluster.KVS_CONFIG);
        try {
            // startup checks are done implicitly in the constructor
            new CassandraClientPool(manager.getConfig());
        } catch (Exception e) {
            return false;
        }
        return true;
    }
}
