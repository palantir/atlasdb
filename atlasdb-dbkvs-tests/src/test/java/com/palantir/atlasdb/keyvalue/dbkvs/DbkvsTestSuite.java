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
package com.palantir.atlasdb.keyvalue.dbkvs;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.palantir.docker.compose.DockerComposition;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.pool.config.ImmutableConnectionConfig;

@RunWith(Suite.class)
@SuiteClasses({
        DbkvsKeyValueServiceTest.class,
        DbkvsKeyValueServiceSerializableTransactionTest.class,
        DbkvsKeyValueServiceSweeperTest.class
})
public class DbkvsTestSuite {

    public static final int POSTGRES_PORT_NUMBER = 5432;
    @ClassRule
    public static final DockerComposition composition = DockerComposition.of("src/test/resources/docker-compose.yml")
            .waitingForHostNetworkedPort(POSTGRES_PORT_NUMBER, toBeOpen())
            .saveLogsTo("container-logs")
            .build();

    static InetSocketAddress POSTGRES_ADDRESS;

    static ImmutablePostgresKeyValueServiceConfig POSTGRES_KVS_CONFIG;

    @BeforeClass
    public static void waitUntilDbkvsIsUp() throws IOException, InterruptedException {
        DockerPort port = composition.hostNetworkedPort(POSTGRES_PORT_NUMBER);
        POSTGRES_ADDRESS = new InetSocketAddress(port.getIp(), port.getExternalPort());


        ImmutableConnectionConfig connectionConfig = ImmutableConnectionConfig.builder()
                .sid("atlas")
                .dbName("atlas")
                .dbLogin("palantir")
                .dbPassword("palantir")
                .dbType(DBType.POSTGRESQL)
                .host(POSTGRES_ADDRESS.getHostName())
                .port(POSTGRES_ADDRESS.getPort())
                .build();

        POSTGRES_KVS_CONFIG = ImmutablePostgresKeyValueServiceConfig.builder()
                .connection(connectionConfig)
                .build();
    }

    private static HealthCheck<DockerPort> toBeOpen() {
        return port -> SuccessOrFailure.fromBoolean(port.isListeningNow(), "" + "" + port + " was not open");
    }
}
