/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.cassandra.multinode;

import static org.hamcrest.Matchers.everyItem;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.Duration;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.ete.Gradle;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class CassandraSchemaLockTest {
    private static final String LOCAL_SERVER_ADDRESS = "http://localhost:3828";
    private static final int THRIFT_PORT_NUMBER_1 = 9160;
    private static final int THRIFT_PORT_NUMBER_2 = 9161;
    private static final int THRIFT_PORT_NUMBER_3 = 9162;

    private static final String CONTAINER_LOGS_DIRECTORY = "container-logs";
    private static final DockerComposeRule CASSANDRA_DOCKER_SETUP = DockerComposeRule.builder()
            .file("src/test/resources/docker-compose-multinode.yml")
            .waitingForService("cassandra1", Container::areAllPortsOpen)
            .waitingForService("cassandra2", Container::areAllPortsOpen)
            .waitingForService("cassandra3", Container::areAllPortsOpen)
            .saveLogsTo(CONTAINER_LOGS_DIRECTORY)
            .build();

    private static final Gradle GRADLE_PREPARE_TASK =
            Gradle.ensureTaskHasRun(":atlasdb-ete-test-utils:buildCassandraImage");
    @ClassRule
    public static final RuleChain CASSANDRA_DOCKER_SET_UP = RuleChain
            .outerRule(GRADLE_PREPARE_TASK)
            .around(CASSANDRA_DOCKER_SETUP);

    private static final CassandraKeyValueServiceConfig KVS_CONFIG = ImmutableCassandraKeyValueServiceConfig.builder()
            .addServers(getCassandraThriftAddressFromPort(THRIFT_PORT_NUMBER_1))
            .addServers(getCassandraThriftAddressFromPort(THRIFT_PORT_NUMBER_2))
            .addServers(getCassandraThriftAddressFromPort(THRIFT_PORT_NUMBER_3))
            .keyspace("atlasdb")
            .replicationFactor(1)
            .credentials(ImmutableCassandraCredentialsConfig.builder()
                    .username("cassandra")
                    .password("cassandra")
                    .build())
            .build();

    private static final Optional<LeaderConfig> leaderConfig = Optional.of(ImmutableLeaderConfig.builder()
            .quorumSize(1)
            .localServer(LOCAL_SERVER_ADDRESS)
            .leaders(ImmutableSet.of(LOCAL_SERVER_ADDRESS))
            .build());

    private static final CassandraKeyValueServiceConfigManager CONFIG_MANAGER = CassandraKeyValueServiceConfigManager
            .createSimpleManager(KVS_CONFIG);

    private final ExecutorService executorService = Executors.newFixedThreadPool(32);

    @BeforeClass
    public static void waitUntilCassandraIsUp() throws IOException, InterruptedException {
        Awaitility.await()
                .atMost(Duration.TWO_MINUTES)
                .pollInterval(Duration.ONE_SECOND)
                .until(canCreateKeyValueService());
    }

    @After
    public void shutdownExecutor() throws InterruptedException {
        executorService.shutdown();
        executorService.awaitTermination(3L, TimeUnit.MINUTES);
    }

    @Test
    public void shouldCreateTablesConsistentlyWithMultipleCassandraNodes() throws Exception {
        TableReference table1 = TableReference.createFromFullyQualifiedName("ns.table1");

        CyclicBarrier barrier = new CyclicBarrier(32);
        for (int i = 0; i < 32; i++) {
            async(() -> {
                CassandraKeyValueService keyValueService =
                        CassandraKeyValueService.create(CONFIG_MANAGER, Optional.absent());
                barrier.await();
                keyValueService.createTable(table1, AtlasDbConstants.GENERIC_TABLE_METADATA);
                return null;
            });
        }
        assertThat(new File(CONTAINER_LOGS_DIRECTORY),
                containsFiles(everyItem(doesNotContainTheColumnFamilyIdMismatchError())));
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    private void async(Callable callable) {
        executorService.submit(callable);
    }

    private static Matcher<File> containsFiles(Matcher<Iterable<File>> fileMatcher) {
        return new FeatureMatcher<File, List<File>>(
                fileMatcher,
                "Directory with files such that",
                "Directory contains") {
            @Override
            protected List<File> featureValueOf(File actual) {
                return ImmutableList.copyOf(actual.listFiles());
            }
        };
    }

    private static Matcher<File> doesNotContainTheColumnFamilyIdMismatchError() {
        return new TypeSafeDiagnosingMatcher<File>() {
            @Override
            protected boolean matchesSafely(File file, Description mismatchDescription) {
                try {
                    List<String> badLines = Files.lines(Paths.get(file.getAbsolutePath()))
                            .filter(line -> line.contains("Column family ID mismatch"))
                            .collect(Collectors.toList());

                    mismatchDescription
                            .appendText("file called " + file.getAbsolutePath() + " which contains lines")
                            .appendValueList("\n", "\n", "", badLines);

                    return badLines.isEmpty();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("a file with no column family ID mismatch errors");
            }
        };
    }

    private static Callable<Boolean> canCreateKeyValueService() {
        return () -> {
            try {
                CassandraKeyValueService.create(
                        CassandraKeyValueServiceConfigManager.createSimpleManager(KVS_CONFIG),
                        leaderConfig);
                return true;
            } catch (Exception e) {
                return false;
            }
        };
    }

    private static InetSocketAddress getCassandraThriftAddressFromPort(int port) {
        DockerPort dockerPort = CASSANDRA_DOCKER_SETUP.hostNetworkedPort(port);
        String hostname = dockerPort.getIp();
        return new InetSocketAddress(hostname, dockerPort.getExternalPort());
    }
}
