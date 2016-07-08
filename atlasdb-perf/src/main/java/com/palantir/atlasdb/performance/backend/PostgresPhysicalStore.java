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
package com.palantir.atlasdb.performance.backend;

import java.io.File;
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

    private static DockerComposeRule docker = null;

    static {
        try {
            File dcFile = PhysicalStoreUtils.writeResourceToTempFile(PostgresPhysicalStore.class,
                    PhysicalStoreConfig.POSTGRES_DOCKER_COMPOSE_PATH);

            docker = DockerComposeRule.builder()
                    .file(dcFile.getAbsolutePath())
                    .waitingForHostNetworkedPort(PhysicalStoreConfig.POSTGRES_PORT_NUMBER, toBeOpen())
                    .saveLogsTo(PhysicalStoreConfig.POSTGRES_DOCKER_LOGS_DIR)
                    .build();

        } catch (IOException e) {
            e.printStackTrace();
        }
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
                .dbName(PhysicalStoreConfig.POSTGRES_DB_NAME)
                .dbLogin(PhysicalStoreConfig.POSTGRES_USER_LOGIN)
                .dbPassword(PhysicalStoreConfig.POSTGRES_USER_PASSWORD)
                .host(docker.containers().ip())
                .port(PhysicalStoreConfig.POSTGRES_PORT_NUMBER)
                .build();

        ImmutableDbKeyValueServiceConfig conf = ImmutableDbKeyValueServiceConfig.builder()
                .connection(connectionConfig)
                .ddl(ImmutablePostgresDdlConfig.builder().build())
                .build();

        return ConnectionManagerAwareDbKvs.create(conf);
    }

    private static HealthCheck<DockerPort> toBeOpen() {
        return port -> SuccessOrFailure.fromBoolean(port.isListeningNow(), "" + "" + port + " was not open");
    }

    @Override
    public void close() throws Exception {
        if (docker != null) {
            docker.after();
        }
    }
}
