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
package com.palantir.atlasdb.keyvalue.cassandra;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.docker.compose.DockerComposition;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthChecks;

@RunWith(Suite.class)
@SuiteClasses({
        CassandraKeyValueServiceSerializableTransactionTest.class,
        CassandraKeyValueServiceTransactionTest.class,
        CassandraKeyValueServiceSweeperTest.class,
        CassandraTimestampTest.class
})
public class CassandraTestSuite {

    @ClassRule
    public static final DockerComposition composition = DockerComposition.of("src/test/resources/docker-compose.yml")
            .waitingForService("cassandra", HealthChecks.toHaveAllPortsOpen()).build();

    static InetSocketAddress CASSANDRA_THRIFT_ADDRESS;
    static CassandraKeyValueServiceConfig CKVS_CONFIG;

    @BeforeClass
    public static void waitUntilCassandraIsUp() throws IOException, InterruptedException {
        DockerPort port = composition.portOnContainerWithInternalMapping("cassandra", CassandraTestConfigs.THRIFT_PORT);
        CASSANDRA_THRIFT_ADDRESS = new InetSocketAddress(port.getIp(), port.getExternalPort());

        CKVS_CONFIG = ImmutableCassandraKeyValueServiceConfig.builder()
                .addServers(CASSANDRA_THRIFT_ADDRESS)
                .poolSize(20)
                .keyspace("atlasdb")
                .ssl(false)
                .replicationFactor(1)
                .mutationBatchCount(10000)
                .mutationBatchSizeBytes(10000000)
                .fetchBatchCount(1000)
                .safetyDisabled(false)
                .autoRefreshNodes(false)
                .build();

    }
}
