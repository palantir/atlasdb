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

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutableDbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutablePostgresDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;
import com.palantir.nexus.db.pool.config.ImmutablePostgresConnectionConfig;

/**
 * Postgres backed physical store for the AtlasDb key-value service.
 *
 * @author mwakerman
 */
public class PostgresPhysicalStore extends PhysicalStore {

    private static final String POSTGRES_DB_NAME             = "atlas";
    private static final int    POSTGRES_PORT_NUMBER         = 5432;
    private static final String POSTGRES_USER_LOGIN          = "palantir";
    private static final String POSTGRES_USER_PASSWORD       = "palantir";
    private static final String POSTGRES_DOCKER_COMPOSE_PATH = "/postgres-docker-compose.yml";
    private static final String POSTGRES_DOCKER_LOGS_DIR     = "container-logs";

    public static PostgresPhysicalStore create() {
            DockerComposeRule docker = DockerComposeRule.builder()
                    .file(getDockerComposeFileAbsoluatePath())
                    .waitingForHostNetworkedPort(POSTGRES_PORT_NUMBER, toBeOpen())
                    .saveLogsTo(POSTGRES_DOCKER_LOGS_DIR)
                    .build();
            return new PostgresPhysicalStore(docker);
    }

    private static String getDockerComposeFileAbsoluatePath() {
        try {
            return PhysicalStores.writeResourceToTempFile(PostgresPhysicalStore.class, POSTGRES_DOCKER_COMPOSE_PATH).getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException("Unable to write docker compose file to a temporary file.", e);
        }
    }

    private static HealthCheck<DockerPort> toBeOpen() {
        return port -> SuccessOrFailure.fromBoolean(port.isListeningNow(), "" + "" + port + " was not open");
    }

    private final DockerComposeRule docker;

    private PostgresPhysicalStore(DockerComposeRule docker) {
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
            System.err.println("Could not run docker compose rule for postgres.");
            e.printStackTrace();
            return null;
        }

        ImmutablePostgresConnectionConfig connectionConfig = ImmutablePostgresConnectionConfig.builder()
                .dbName(POSTGRES_DB_NAME)
                .dbLogin(POSTGRES_USER_LOGIN)
                .dbPassword(POSTGRES_USER_PASSWORD)
                .host(docker.containers().ip())
                .port(POSTGRES_PORT_NUMBER)
                .build();

        ImmutableDbKeyValueServiceConfig conf = ImmutableDbKeyValueServiceConfig.builder()
                .connection(connectionConfig)
                .ddl(ImmutablePostgresDdlConfig.builder().build())
                .build();

        return ConnectionManagerAwareDbKvs.create(conf);
    }

    @Override
    public void close() throws Exception {
        if (docker != null) {
            docker.after();
        }
    }
}
