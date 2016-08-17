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

import static java.util.stream.Collectors.toList;

import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.MatcherAssert.assertThat;

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

import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.Duration;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.ete.Gradle;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.docker.compose.DockerComposition;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;

public class CassandraSchemaLockTest {
    public static final int THRIFT_PORT_NUMBER = 9160;
    public static final DockerComposition composition = DockerComposition.of("src/test/resources/docker-compose-multinode.yml")
            .waitingForHostNetworkedPort(THRIFT_PORT_NUMBER, toBeOpen())
            .saveLogsTo("container-logs-multinode")
            .build();

    public static final Gradle GRADLE_PREPARE_TASK = Gradle.ensureTaskHasRun(":atlasdb-ete-test-utils:buildCassandraImage");

    @ClassRule
    public static final RuleChain CASSANDRA_DOCKER_SET_UP = RuleChain.outerRule(GRADLE_PREPARE_TASK).around(composition);

    static InetSocketAddress CASSANDRA_THRIFT_ADDRESS;

    static ImmutableCassandraKeyValueServiceConfig CASSANDRA_KVS_CONFIG;

    static Optional<LeaderConfig> LEADER_CONFIG;
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

    @Test
    public void shouldCreateTablesConsistentlyWithMultipleCassandraNodes() throws Exception {
        TableReference table1 = TableReference.createFromFullyQualifiedName("ns.table1");

        CyclicBarrier barrier = new CyclicBarrier(32);
        try {
            for(int i=0; i < 32; i++) {
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
        executorService.shutdown();
        executorService.awaitTermination(5L, TimeUnit.MINUTES);

        assertThat(new File("container-logs-multinode"), containsFiles(everyItem(doesNotContainTheColumnFamilyIdMismatchError())));
    }

    private Matcher<File> containsFiles(Matcher<Iterable<File>> fileMatcher) {
        return new FeatureMatcher<File, List<File>>(fileMatcher, "Directory with files such that", "Directory contains") {
            @Override
            protected List<File> featureValueOf(File actual) {
                return Lists.newArrayList(actual.listFiles());
            }
        };
    }

    private Matcher<File> doesNotContainTheColumnFamilyIdMismatchError() {
        return new TypeSafeDiagnosingMatcher<File>() {
            @Override
            protected boolean matchesSafely(File file, Description mismatchDescription) {
                try {
                    List<String> badLines = Files.lines(Paths.get(file.getAbsolutePath()))
                            .filter(line -> line.contains("Column family ID mismatch"))
                            .collect(toList());

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
