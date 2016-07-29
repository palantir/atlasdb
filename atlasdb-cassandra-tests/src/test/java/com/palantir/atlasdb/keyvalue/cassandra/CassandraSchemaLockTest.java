/**
 * Copyright 2016 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.cassandra;

import static com.google.common.base.Optional.absent;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.Duration;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.docker.compose.DockerComposition;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;

@Ignore // Used to detect duplicate tables (the "Cassandra table creation bug"), but currently requires manual verification.
public class CassandraSchemaLockTest {

    public static final int THRIFT_PORT_NUMBER = 9160;
    @ClassRule
    public static final DockerComposition composition = DockerComposition.of("src/test/resources/docker-compose-multinode.yml")
            .waitingForHostNetworkedPort(THRIFT_PORT_NUMBER, toBeOpen())
            .saveLogsTo("container-logs-multinode")
            .build();

    static InetSocketAddress CASSANDRA_THRIFT_ADDRESS;

    static ImmutableCassandraKeyValueServiceConfig CASSANDRA_KVS_CONFIG_NO_LOCK_LEADER;

    static ImmutableCassandraKeyValueServiceConfig CASSANDRA_KVS_CONFIG;

    static Optional<LeaderConfig> LEADER_CONFIG;
    private final Cell TABLE_CELL = Cell.create(PtBytes.toBytes("row0"), PtBytes.toBytes("col0"));
    private final byte[] TABLE_VALUE = PtBytes.toBytes("xyz");
    private final ExecutorService executorService = Executors.newFixedThreadPool(32);
    static private CassandraKeyValueServiceConfigManager CONFIG_MANAGER;


    @BeforeClass
    public static void waitUntilCassandraIsUp() throws IOException, InterruptedException {
        DockerPort port = composition.hostNetworkedPort(THRIFT_PORT_NUMBER);
        String hostname = port.getIp();
        CASSANDRA_THRIFT_ADDRESS = new InetSocketAddress(hostname, port.getExternalPort());

        CASSANDRA_KVS_CONFIG = ImmutableCassandraKeyValueServiceConfig.builder()
                .addServers(CASSANDRA_THRIFT_ADDRESS)
                .poolSize(20)
                .keyspace("atlasdb")
                .credentials(ImmutableCassandraCredentialsConfig.builder()
                        .username("cassandra")
                        .password("cassandra")
                        .build())
                .ssl(false)
                .replicationFactor(1)
                .mutationBatchCount(10000)
                .mutationBatchSizeBytes(10000000)
                .fetchBatchCount(1000)
                .safetyDisabled(false)
                .autoRefreshNodes(false)
                .build();

        CONFIG_MANAGER = CassandraKeyValueServiceConfigManager.createSimpleManager(CASSANDRA_KVS_CONFIG);

        LEADER_CONFIG = Optional.of(ImmutableLeaderConfig
                .builder()
                .quorumSize(1)
                .localServer(hostname)
                .leaders(ImmutableSet.of(hostname))
                .build());

        Awaitility.await()
                .atMost(Duration.ONE_MINUTE)
                .pollInterval(Duration.ONE_SECOND)
                .until(canCreateKeyValueService());
    }

    /*
    To correctly detect duplicate tables:
     1. run this test
     2. after table ns.table1 has been created, check your Cassandra logs for mismatch exceptions: "docker logs seed | grep mismatch"
     3. restart your docker machine: "docker restart <container_id>
     4. check the list of tables in the data directory: "docker exec -it seed  ls /var/lib/cassandra/data/atlasdb && echo "\n" && docker exec -it cassandra1 ls /var/lib/cassandra/data/atlasdb && echo "\n" && docker exec -it cassandra2  ls /var/lib/cassandra/data/atlasdb"
     If you see exceptions in step 2, then step 4 should yield multiple tables of the form "ns__table1*" on at least one node.
     */
    @Test
    public void shouldCreateTablesWithMultipleCassandraNodes() throws Exception {

        TableReference table1 = TableReference.createFromFullyQualifiedName("ns.table1");

        CyclicBarrier barrier = new CyclicBarrier(32);


        try {
            for(int i=0; i < 100; i++) {
                async(() -> {
                    CassandraKeyValueService keyValueService = CassandraKeyValueService.create(CONFIG_MANAGER, Optional.absent());
                    barrier.await();
                    keyValueService.createTable(table1, AtlasDbConstants.GENERIC_TABLE_METADATA);
                    return null;
                });
            }
        } catch (Exception e) {
            throw e;
        }

        KeyValueService keyValueService = CassandraKeyValueService.create(CONFIG_MANAGER, absent());

        executorService.shutdown();
        executorService.awaitTermination(5L, TimeUnit.MINUTES);

        keyValueService.put(table1, ImmutableMap.of(TABLE_CELL, TABLE_VALUE), 127L);
        Map<Cell, Value> cellValueMap = keyValueService.get(table1, ImmutableMap.of(Cell.create(PtBytes.toBytes("row0"), PtBytes.toBytes("col0")), 130L));

        System.out.println("We got from table1 : " + cellValueMap.size());
    }


    private static Callable<Boolean> canCreateKeyValueService() {
        return () -> {
            try {
                CassandraKeyValueService.create(CassandraKeyValueServiceConfigManager.createSimpleManager(CASSANDRA_KVS_CONFIG), LEADER_CONFIG);
                return true;
            } catch (Exception e) {
                return false;
            }
        };
    }

    protected void async(Callable callable) {
        executorService.submit(callable);
    }

    private static HealthCheck<DockerPort> toBeOpen() {
        return port -> SuccessOrFailure.fromBoolean(port.isListeningNow(), "" + "" + port + " was not open");
    }
}
