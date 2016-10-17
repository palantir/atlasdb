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

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.Duration;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.db.oracle.NativeOracleJdbcHandler;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.nexus.db.pool.config.ConnectionConfig;
import com.palantir.nexus.db.pool.config.ImmutableMaskedValue;
import com.palantir.nexus.db.pool.config.ImmutableOracleConnectionConfig;

@RunWith(Suite.class)
@SuiteClasses({
        DbkvsOracleKeyValueServiceTest.class,
        DbkvsOracleSerializableTransactionTest.class,
        DbkvsOracleSweeperTest.class
})
public final class DbkvsOracleTestSuite {
    private static final int ORACLE_PORT_NUMBER = 1521;

    private DbkvsOracleTestSuite() {
        // Test suite
    }

    @ClassRule
    public static final DockerComposeRule docker = DockerComposeRule.builder()
            .file("src/test/resources/docker-compose.oracle.yml")
            .waitingForService("oracle", Container::areAllPortsOpen)
            .saveLogsTo("container-logs")
            .skipShutdown(true)
            .build();

    @BeforeClass
    public static void waitUntilDbkvsIsUp() throws InterruptedException {
        Awaitility.await()
                .atMost(Duration.FIVE_MINUTES)
                .pollInterval(Duration.TEN_SECONDS)
                .until(canCreateKeyValueService());
    }

    public static DbKeyValueServiceConfig getKvsConfig() {
        DockerPort port = docker.containers()
                .container("oracle")
                .port(ORACLE_PORT_NUMBER);

        InetSocketAddress oracleAddress = new InetSocketAddress(port.getIp(), port.getExternalPort());

        ConnectionConfig connectionConfig = ImmutableOracleConnectionConfig.builder()
                .dbLogin("palantir")
                .dbPassword(ImmutableMaskedValue.of("palpal"))
                .sid("palantir")
                .host(oracleAddress.getHostName())
                .port(oracleAddress.getPort())
                .build();

        return ImmutableDbKeyValueServiceConfig.builder()
                .connection(connectionConfig)
                .ddl(ImmutableOracleDdlConfig.builder()
                        .overflowMigrationState(OverflowMigrationState.FINISHED)
                        .jdbcHandler(new NativeOracleJdbcHandler())
                        .build())
                .build();
    }

    private static Callable<Boolean> canCreateKeyValueService() {
        return () -> {
            ConnectionManagerAwareDbKvs kvs = null;
            try {
                kvs = ConnectionManagerAwareDbKvs.create(getKvsConfig());
                return kvs.getConnectionManager().getConnection().isValid(5);
            } catch (Exception e) {
                return false;
            } finally {
                if (kvs != null) {
                    kvs.close();
                }
            }
        };
    }
}
