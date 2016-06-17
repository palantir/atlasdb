/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.performance.cli.backend;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.dbkvs.ImmutablePostgresKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.DbKvs;
import com.palantir.docker.compose.DockerComposition;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.pool.config.ImmutableConnectionConfig;

public class PostgresStore extends PhysicalStore {

    private static final int POSTGRES_PORT_NUMBER = 5432;

    private static DockerComposition composition;

    @Override
    public KeyValueService connect() {
        composition = dockerCompose();

        DockerPort port = composition.hostNetworkedPort(POSTGRES_PORT_NUMBER);
        InetSocketAddress addr = new InetSocketAddress(port.getIp(), port.getExternalPort());

        ImmutableConnectionConfig connectionConfig = ImmutableConnectionConfig.builder()
                .sid("atlas")
                .dbName("atlas")
                .dbLogin("palantir")
                .dbPassword("palantir")
                .dbType(DBType.POSTGRESQL)
                .host(addr.getHostName())
                .port(addr.getPort())
                .build();
        ImmutablePostgresKeyValueServiceConfig config = ImmutablePostgresKeyValueServiceConfig.builder()
                .connection(connectionConfig)
                .build();

        return DbKvs.create(config);
    }

    private DockerComposition dockerCompose() {
         DockerComposition comp = DockerComposition.of("src/main/resources/postgres-docker-compose.yml")
                .waitingForHostNetworkedPort(POSTGRES_PORT_NUMBER, toBeOpen())
                .saveLogsTo("container-logs")
                .build();
        try {
            comp.before();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        return comp;
    }

    private static HealthCheck<DockerPort> toBeOpen() {
        return port -> SuccessOrFailure.fromBoolean(port.isListeningNow(), "" + "" + port + " was not open");
    }

    @Override
    public void close() throws Exception {
        composition.after();
    }
}
