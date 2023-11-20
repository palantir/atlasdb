/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.cassandra.multinode;

import com.google.common.base.Throwables;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.containers.ThreeNodeCassandraCluster;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.refreshable.Refreshable;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.awaitility.Awaitility;
import org.junit.rules.ExternalResource;

public class NodesDownTestSetup extends ExternalResource {
    private static final int CASSANDRA_THRIFT_PORT = 9160;
    private static final CassandraKeyValueServiceConfig CONFIG = ImmutableCassandraKeyValueServiceConfig.copyOf(
                    ThreeNodeCassandraCluster.KVS_CONFIG)
            .withSchemaMutationTimeoutMillis(3_000);
    private static final Refreshable<CassandraKeyValueServiceRuntimeConfig> RUNTIME_CONFIG =
            ThreeNodeCassandraCluster.KVS_RUNTIME_CONFIG;

    public final Containers CONTAINERS = new Containers(NodesDownTestSetup.class).with(new ThreeNodeCassandraCluster());

    @Override
    protected void before() throws Throwable {
        CONTAINERS.before();
    }

    @Override
    protected void after() {
        AbstractDegradedClusterTest.closeAll();
        CONTAINERS.after();
    }

    void initializeKvsAndDegradeCluster(List<Class<?>> tests, List<String> nodesToKill) throws Exception {
        for (Class<?> test : tests) {
            test.getMethod("initialize", CassandraKeyValueService.class)
                    .invoke(test.getDeclaredConstructor().newInstance(), createKvs(test));
        }
        degradeCassandraCluster(nodesToKill);
    }

    private static CassandraKeyValueService createKvs(Class<?> testClass) {
        return CassandraKeyValueServiceImpl.createForTesting(getConfig(testClass), RUNTIME_CONFIG);
    }

    static CassandraKeyValueServiceConfig getConfig(Class<?> testClass) {
        return ImmutableCassandraKeyValueServiceConfig.builder()
                .from(CONFIG)
                .keyspace(testClass.getSimpleName())
                .build();
    }

    private void degradeCassandraCluster(List<String> nodesToKill) {
        nodesToKill.forEach(containerName -> {
            try {
                killCassandraContainer(containerName);
            } catch (IOException | InterruptedException e) {
                Throwables.propagate(e);
            }
        });
    }

    private void killCassandraContainer(String containerName) throws IOException, InterruptedException {
        CONTAINERS.getContainer(containerName).kill();
        DockerPort containerPort = new DockerPort(containerName, CASSANDRA_THRIFT_PORT, CASSANDRA_THRIFT_PORT);
        Awaitility.waitAtMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofSeconds(2))
                .until(() -> !containerPort.isListeningNow());
    }
}
