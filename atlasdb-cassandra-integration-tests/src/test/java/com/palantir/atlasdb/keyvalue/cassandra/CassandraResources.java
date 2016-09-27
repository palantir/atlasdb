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
package com.palantir.atlasdb.keyvalue.cassandra;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Callable;

import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.Duration;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.ete.Gradle;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;

class CassandraResources extends ExternalResource {
    private final Logger log = LoggerFactory.getLogger(CassandraResources.class);
    private final int THRIFT_PORT_NUMBER = 9160;
    private final Gradle GRADLE_PREPARE_TASK = Gradle.ensureTaskHasRun(
            ":atlasdb-ete-test-utils:buildCassandraImage");
    private final DockerComposeRule docker = DockerComposeRule.builder()
            .file("src/test/resources/docker-compose.yml")
            .waitingForHostNetworkedPort(THRIFT_PORT_NUMBER, toBeOpen())
            .saveLogsTo("container-logs")
            .build();
    private final RuleChain CASSANDRA_DOCKER_SET_UP = RuleChain.outerRule(GRADLE_PREPARE_TASK).around(docker);

    private static int refCount = 0;
    private static CassandraResources currentInstance;

    static InetSocketAddress CASSANDRA_THRIFT_ADDRESS;
    static ImmutableCassandraKeyValueServiceConfig CASSANDRA_KVS_CONFIG;
    static Optional<LeaderConfig> LEADER_CONFIG;

    static synchronized CassandraResources getCassandraResource() {
        if (refCount == 0) {
            // currentInstance either hasn't been created yet, or after was called on it - create a new one
            currentInstance = new CassandraResources();
        }
        return currentInstance;
    }

    private CassandraResources() {
        // nothing to initialize
    }

    @Override
    protected void before() {
        synchronized (CassandraResources.class) {
            try {
                if (refCount == 0) {
                    try {
                        waitUntilCassandraIsUp();
                    } catch (Throwable e) {
                        log.error("Exception: {}", e);
                    }
                }
            } finally {
                refCount++;
            }
        }
    }

    @Override
    protected void after() {
        synchronized (CassandraResources.class) {
            Preconditions.checkState(refCount > 0, "No more references to kill");
            refCount--;
            if (refCount == 0) {
                log.info("Killed all refs to Cassandra resources");
            }
        }
    }

    @Override
    public Statement apply(Statement base, Description description) {
        synchronized (CassandraResources.class) {
            Statement cassandraResources = super.apply(base, description);
            return refCount == 0 ? CASSANDRA_DOCKER_SET_UP.apply(cassandraResources, description) : cassandraResources;
        }
    }

    private void waitUntilCassandraIsUp() throws IOException, InterruptedException {
        System.out.println(currentInstance == this);
        System.out.println(currentInstance);
        System.out.println(this);
        DockerPort port = docker.hostNetworkedPort(THRIFT_PORT_NUMBER);
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

    private Callable<Boolean> canCreateKeyValueService() {
        return () -> {
            try {
                CassandraKeyValueService.create(CassandraKeyValueServiceConfigManager.createSimpleManager(
                        CASSANDRA_KVS_CONFIG), LEADER_CONFIG);
                return true;
            } catch (Exception e) {
                return false;
            }
        };
    }

    private HealthCheck<DockerPort> toBeOpen() {
        return port -> SuccessOrFailure.fromBoolean(port.isListeningNow(), port + " was not open");
    }

}
