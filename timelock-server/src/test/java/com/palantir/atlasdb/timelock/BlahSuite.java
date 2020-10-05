/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.timelock;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
        Blah.class
        })
public final class BlahSuite {
    private static final int POSTGRES_PORT_NUMBER = 5432;

    private BlahSuite() {
        // Test suite
    }

//    @ClassRule
//    public static final DockerComposeRule docker = DockerComposeRule.builder()
//            .file("src/test/resources/docker-compose.yml")
//            .waitingForService("blah-postgres", Container::areAllPortsOpen)
//            .saveLogsTo(LogDirectory.circleAwareLogDirectory(BlahSuite.class))
//            .shutdownStrategy(ShutdownStrategy.AGGRESSIVE_WITH_NETWORK_CLEANUP)
//            .build();

//    @BeforeClass
//    public static void waitUntilDbkvsIsUp() throws InterruptedException {
//        Awaitility.await()
//                .atMost(Duration.ONE_MINUTE)
//                .pollInterval(Duration.ONE_SECOND)
//                .until(canCreateKeyValueService());
//    }

    public static TemplateVariables.DbKvsConnectionConfig getKvsConfig() {
//        DockerPort port = docker.containers()
//                .container("blah-postgres")
//                .port(POSTGRES_PORT_NUMBER);
//
//        InetSocketAddress postgresAddress = new InetSocketAddress(port.getIp(), port.getExternalPort());

        return ImmutableTemplateVariables.DbKvsConnectionConfig.builder()
                .dbName("atlas")
                .dbLogin("palantir")
                .dbPassword("palantir")
                .host("localhost")
                .port(5432)
                .build();
    }

//    private static Callable<Boolean> canCreateKeyValueService() {
//        return () -> {
//            ConnectionManagerAwareDbKvs kvs = null;
//            try {
//                kvs = createKvs();
//                return kvs.getConnectionManager().getConnection().isValid(5);
//            } catch (Exception ex) {
//                if (ex.getMessage().contains("The connection attempt failed.")
//                        || ex.getMessage().contains("the database system is starting up")) {
//                    return false;
//                } else {
//                    throw ex;
//                }
//            } finally {
//                if (kvs != null) {
//                    kvs.close();
//                }
//            }
//        };
//    }

//    public static ConnectionManagerAwareDbKvs createKvs() {
//        return ConnectionManagerAwareDbKvs.create(getKvsConfig());
//    }
}
