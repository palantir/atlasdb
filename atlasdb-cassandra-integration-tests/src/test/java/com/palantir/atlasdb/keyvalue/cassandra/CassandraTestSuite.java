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
import com.palantir.atlasdb.testing.DockerProxyRule;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.logging.LogDirectory;

@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
@RunWith(Suite.class)
@SuiteClasses({
        CassandraKeyValueServiceTransactionIntegrationTest.class,
        CassandraClientPoolIntegrationTest.class,
        CassandraConnectionIntegrationTest.class,
        CassandraKeyValueServiceTableCreationIntegrationTest.class,
        CassandraKeyValueServiceSerializableTransactionIntegrationTest.class,
        CassandraKeyValueServiceSweeperIntegrationTest.class,
        CassandraTimestampIntegrationTest.class,
        CassandraKeyValueServiceIntegrationTest.class,
        HeartbeatServiceIntegrationTest.class,
        SchemaMutationLockIntegrationTest.class,
        SchemaMutationLockTablesIntegrationTest.class
        })
public final class CassandraTestSuite {
    private static final DockerComposeRule DOCKER = DockerComposeRule.builder()
            .file("src/test/resources/docker-compose.yml")
            .saveLogsTo(LogDirectory.circleAwareLogDirectory(CassandraTestSuite.class))
            .build();
    private static final DockerProxyRule DOCKER_PROXY_RULE = new DockerProxyRule(
            DOCKER.projectName(),
            CassandraTestSuite.class);

    @ClassRule
    public static final RuleChain CASSANDRA_DOCKER_SET_UP = RuleChain.outerRule(DOCKER)
            .around(DOCKER_PROXY_RULE);

    static final ImmutableCassandraKeyValueServiceConfig KVS_CONFIG = ImmutableCassandraKeyValueServiceConfig.builder()
            .addServers(new InetSocketAddress("cassandra", 9160))
            .poolSize(20)
            .keyspace("atlasdb")
            .credentials(ImmutableCassandraCredentialsConfig.builder()
                    .username("cassandra")
                    .password("cassandra")
                    .build())
            .replicationFactor(1)
            .mutationBatchCount(10000)
            .mutationBatchSizeBytes(10000000)
            .fetchBatchCount(1000)
            .safetyDisabled(false)
            .autoRefreshNodes(false)
            .build();

    static final Optional<LeaderConfig> LEADER_CONFIG = Optional.of(ImmutableLeaderConfig
            .builder()
            .quorumSize(1)
            .localServer("localhost")
            .leaders(ImmutableSet.of("localhost"))
            .build());

    @BeforeClass
    public static void waitUntilCassandraIsUp() throws IOException, InterruptedException {
        Awaitility.await()
                .atMost(Duration.ONE_MINUTE)
                .pollInterval(Duration.ONE_SECOND)
                .until(canCreateKeyValueService());
    }

    private static Callable<Boolean> canCreateKeyValueService() {
        return () -> {
            try {
                CassandraKeyValueService.create(
                        CassandraKeyValueServiceConfigManager.createSimpleManager(KVS_CONFIG),
                        LEADER_CONFIG);
                return true;
            } catch (Exception e) {
                return false;
            }
        };
    }
}
