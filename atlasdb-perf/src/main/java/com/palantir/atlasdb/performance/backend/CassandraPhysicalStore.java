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
 *
 */

package com.palantir.atlasdb.performance.backend;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Callable;

import org.joda.time.Duration;

import com.google.common.base.Optional;
import com.jayway.awaitility.Awaitility;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;

/**
 * Cassandra backed physical store for the AtlasDb key-value service.
 *
 * @author mwakerman
 */
public final class CassandraPhysicalStore extends PhysicalStore {

    private static final String KEYSPACE                     = "atlasdb";
    private static final int    THRIFT_PORT_NUMBER           = 9160;
    private static final String CASSANDRA_USERNAME           = "cassandra";
    private static final String CASSANDRA_PASSWORD           = "cassandra";
    private static final String CASSANDRA_DOCKER_COMPOSE_PATH = "/cassandra-docker-compose.yml";
    private static final String DOCKER_LOGS_DIR              = "container-logs";

    public static CassandraPhysicalStore create() {
        DockerComposeRule docker = DockerComposeRule.builder()
                .file(getDockerComposeFileAbsolutePath())
                .waitingForHostNetworkedPort(THRIFT_PORT_NUMBER, toBeOpen(), Duration.standardMinutes(1))
                .saveLogsTo(DOCKER_LOGS_DIR)
                .build();


        return new CassandraPhysicalStore(docker);
    }

    private static String getDockerComposeFileAbsolutePath() {
        try {
            return PhysicalStores
                    .writeResourceToTempFile(CassandraPhysicalStore.class, CASSANDRA_DOCKER_COMPOSE_PATH)
                    .getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException("Unable to write docker compose file to a temporary file.", e);
        }
    }

    private static HealthCheck<DockerPort> toBeOpen() {
        return port -> SuccessOrFailure.fromBoolean(port.isListeningNow(), "" + "" + port + " was not open");
    }

    private final DockerComposeRule docker;

    private CassandraPhysicalStore(DockerComposeRule docker) {
        this.docker = docker;
    }

    @Override
    public KeyValueService connect() {
        try {
            if (docker == null) {
                throw new IllegalStateException("Docker compose rule cannot be run, is null.");
            } else {
                docker.before();
            }
        } catch (IOException | InterruptedException | IllegalStateException e) {
            throw new RuntimeException("Could not run docker compose rule for cassandra.", e);
        }

        ImmutableCassandraKeyValueServiceConfig connectionConfig = ImmutableCassandraKeyValueServiceConfig.builder()
                .addServers(new InetSocketAddress(docker.containers().ip(), THRIFT_PORT_NUMBER))
                .poolSize(20)
                .keyspace(KEYSPACE)
                .credentials(ImmutableCassandraCredentialsConfig.builder()
                        .username(CASSANDRA_USERNAME)
                        .password(CASSANDRA_PASSWORD)
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
                .atMost(com.jayway.awaitility.Duration.ONE_MINUTE)
                .pollInterval(com.jayway.awaitility.Duration.FIVE_SECONDS)
                .until(canCreateKeyValueService(connectionConfig));

        return CassandraKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(connectionConfig),
                Optional.absent());
    }

    private static Callable<Boolean> canCreateKeyValueService(CassandraKeyValueServiceConfig config) {
        return () -> {
            try {
                CassandraKeyValueService.create(
                        CassandraKeyValueServiceConfigManager.createSimpleManager(config),
                        Optional.absent());
                return true;
            } catch (Exception e) {
                return false;
            }
        };
    }

    @Override
    public void close() throws Exception {
        if (docker != null) {
            docker.after();
        }
    }
}
