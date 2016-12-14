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
package com.palantir.atlasdb.calcite;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.google.common.base.Throwables;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.Duration;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbConfigs;
import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.configuration.ShutdownStrategy;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.logging.LogDirectory;

@RunWith(Suite.class)
@SuiteClasses({
        SmokeTests.class,
        SimpleQueryTests.class
})
public final class AtlasJdbcTestSuite {
    private static final int POSTGRES_PORT_NUMBER = 5432;
    private static final String ATLAS_CONFIG_TEMPLATE = "src/test/resources/atlas-config-template.yml";

    private static AtlasDbServices services;

    private AtlasJdbcTestSuite() {
        // Test suite
    }

    @ClassRule
    public static final DockerComposeRule docker = DockerComposeRule.builder()
            .file("src/test/resources/docker-compose.yml")
            .waitingForService("postgres", Container::areAllPortsOpen)
            .saveLogsTo(LogDirectory.circleAwareLogDirectory(AtlasJdbcTestSuite.class))
            .shutdownStrategy(ShutdownStrategy.AGGRESSIVE_WITH_NETWORK_CLEANUP)
            .build();

    @BeforeClass
    public static void startDatabaseAndAtlas() throws InterruptedException, IOException {
        Awaitility.await()
                .atMost(Duration.ONE_MINUTE)
                .pollInterval(Duration.ONE_SECOND)
                .until(canCreateKeyValueService());
        connect();
        services = AtlasSchemaFactory.getLastKnownAtlasServices();
    }

    public static AtlasDbServices getAtlasDbServices() {
        return services;
    }

    public static Connection connect() {
        try {
            String uri = String.format(
                    "jdbc:calcite:schemaFactory=%s;schema.%s=%s;lex=MYSQL_ANSI;schema=atlas",
                    AtlasSchemaFactory.class.getName(),
                    AtlasSchemaFactory.ATLAS_CONFIG_FILE_KEY,
                    getAtlasConfigFile().getPath());
            return DriverManager.getConnection(uri);
        } catch (IOException | SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    private static File getAtlasConfigFile() throws IOException {
        InetSocketAddress postgresAddress = getPostgresAddress();
        String configTemplate = FileUtils.readFileToString(new File(ATLAS_CONFIG_TEMPLATE));
        File atlasConfig = File.createTempFile(ATLAS_CONFIG_TEMPLATE, ".tmp");
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(atlasConfig))) {
            bw.write(String.format(configTemplate, postgresAddress.getHostName(), postgresAddress.getPort()));
        }
        return atlasConfig;
    }

    public static InetSocketAddress getPostgresAddress() {
        DockerPort port = docker.containers()
                .container("postgres")
                .port(POSTGRES_PORT_NUMBER);
        return new InetSocketAddress(port.getIp(), port.getExternalPort());
    }

    private static Callable<Boolean> canCreateKeyValueService() {
        return () -> {
            ConnectionManagerAwareDbKvs kvs = null;
            try {
                AtlasDbConfig config = AtlasDbConfigs.load(getAtlasConfigFile());
                kvs = ConnectionManagerAwareDbKvs.create((DbKeyValueServiceConfig) config.keyValueService());
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
