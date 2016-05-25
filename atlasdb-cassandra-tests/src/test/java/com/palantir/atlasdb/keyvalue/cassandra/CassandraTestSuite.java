/**
 * Copyright 2015 Palantir Technologies
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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Callable;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.Duration;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.docker.compose.DockerComposition;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;

@RunWith(Suite.class)
@SuiteClasses({
        CassandraConnectionTest.class,
        CassandraKeyValueServiceSerializableTransactionTest.class,
        CassandraKeyValueServiceSweeperTest.class,
        CassandraTimestampTest.class,
        CassandraKeyValueServiceTest.class,
        CassandraDbLockTest.class,
        CassandraLegacyLockTest.class
})
public class CassandraTestSuite {

    public static final int THRIFT_PORT_NUMBER = 9160;
    @ClassRule
    public static final DockerComposition composition = DockerComposition.of("src/test/resources/docker-compose.yml")
            .waitingForHostNetworkedPort(THRIFT_PORT_NUMBER, toBeOpen())
            .saveLogsTo("container-logs")
            .build();

    static InetSocketAddress CASSANDRA_THRIFT_ADDRESS;

    static ImmutableCassandraKeyValueServiceConfig CASSANDRA_KVS_CONFIG;

    @BeforeClass
    public static void waitUntilCassandraIsUp() throws IOException, InterruptedException {
        DockerPort port = composition.hostNetworkedPort(THRIFT_PORT_NUMBER);
        CASSANDRA_THRIFT_ADDRESS = new InetSocketAddress(port.getIp(), port.getExternalPort());

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

        Awaitility.await()
                .atMost(Duration.ONE_MINUTE)
                .pollInterval(Duration.ONE_SECOND)
                .until(canCreateKeyValueService());
    }

    private static Callable<Boolean> canCreateKeyValueService() {
        return new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try {
                    CassandraKeyValueService.create(CassandraKeyValueServiceConfigManager.createSimpleManager(CASSANDRA_KVS_CONFIG));
                    return true;
                } catch (Exception e) {
                    return false;
                }
            }
        };
    }

    private static HealthCheck<DockerPort> toBeOpen() {
        return port -> SuccessOrFailure.fromBoolean(port.isListeningNow(), "" + "" + port + " was not open");
    }
}
