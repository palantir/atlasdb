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
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.Duration;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.ete.Gradle;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;

@RunWith(Suite.class)
@SuiteClasses({
        CassandraConnectionTest.class,
        CassandraKeyValueServiceTableCreationTest.class,
        CassandraKeyValueServiceSerializableTransactionTest.class,
        CassandraKeyValueServiceSweeperTest.class,
        CassandraTimestampTest.class,
        CassandraKeyValueServiceTest.class,
        SchemaMutationLockTest.class,
        SchemaMutationLockTablesTest.class,
})
public class CassandraTestSuite {

    public static final int THRIFT_PORT_NUMBER = 9160;
    public static final DockerComposeRule docker = DockerComposeRule.builder()
            .file("src/test/resources/docker-compose.yml")
            .waitingForHostNetworkedPort(THRIFT_PORT_NUMBER, toBeOpen())
            .saveLogsTo("container-logs")
            .build();

    public static final Gradle GRADLE_PREPARE_TASK = Gradle.ensureTaskHasRun(":atlasdb-ete-test-utils:buildCassandraImage");

    @ClassRule
    public static final RuleChain CASSANDRA_DOCKER_SET_UP = RuleChain.outerRule(GRADLE_PREPARE_TASK).around(docker);

    static InetSocketAddress CASSANDRA_THRIFT_ADDRESS;

    static ImmutableCassandraKeyValueServiceConfig CASSANDRA_KVS_CONFIG;

    static Optional<LeaderConfig> LEADER_CONFIG;

    @BeforeClass
    public static void waitUntilCassandraIsUp() throws IOException, InterruptedException {
        DockerPort port = docker.hostNetworkedPort(THRIFT_PORT_NUMBER);
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

    private static Callable<Boolean> canCreateKeyValueService() {
        return new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try {
                    CassandraKeyValueService.create(CassandraKeyValueServiceConfigManager.createSimpleManager(CASSANDRA_KVS_CONFIG), LEADER_CONFIG);
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
