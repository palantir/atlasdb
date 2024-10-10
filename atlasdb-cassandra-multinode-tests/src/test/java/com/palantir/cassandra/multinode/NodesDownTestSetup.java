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

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

import com.google.common.base.Throwables;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.containers.ThreeNodeCassandraCluster;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;
import com.palantir.atlasdb.limiter.NoOpAtlasClientLimiter;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.refreshable.Refreshable;
import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.reflections.Reflections;

public class NodesDownTestSetup implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {
    private static volatile boolean started = false;
    private static final int CASSANDRA_THRIFT_PORT = 9160;
    private static final CassandraKeyValueServiceConfig CONFIG = ImmutableCassandraKeyValueServiceConfig.copyOf(
                    ThreeNodeCassandraCluster.KVS_CONFIG)
            .withSchemaMutationTimeoutMillis(3_000);
    private static final Refreshable<CassandraKeyValueServiceRuntimeConfig> RUNTIME_CONFIG =
            ThreeNodeCassandraCluster.KVS_RUNTIME_CONFIG;

    public static final Containers CONTAINERS =
            new Containers(NodesDownTestSetup.class).with(new ThreeNodeCassandraCluster());

    @Override
    public synchronized void beforeAll(ExtensionContext context) throws Exception {
        if (!started) {
            started = true;
            CONTAINERS.beforeAll(context);
            for (Class<?> test : getNodesDownTestClasses()) {
                test.getMethod("initialize", CassandraKeyValueService.class)
                        .invoke(test.getDeclaredConstructor().newInstance(), createKvs(test));
            }
            context.getRoot().getStore(GLOBAL).put("NodesDownTestSetup", this);
        }
    }

    @Override
    public void close() {
        AbstractDegradedClusterTest.closeAll();
        CONTAINERS.afterAll();
    }

    private static Set<Class<?>> getNodesDownTestClasses() {
        Reflections reflections = new Reflections("com.palantir.cassandra.multinode");
        return reflections.getTypesAnnotatedWith(NodesDownTestClass.class);
    }

    private static CassandraKeyValueService createKvs(Class<?> testClass) {
        return CassandraKeyValueServiceImpl.createForTesting(
                getConfig(testClass), RUNTIME_CONFIG, new NoOpAtlasClientLimiter());
    }

    static CassandraKeyValueServiceConfig getConfig(Class<?> testClass) {
        return ImmutableCassandraKeyValueServiceConfig.builder()
                .from(CONFIG)
                .keyspace(testClass.getSimpleName())
                .build();
    }

    public static void degradeCassandraCluster(String nodeToKill) {
        try {
            killCassandraContainer(nodeToKill);
        } catch (IOException | InterruptedException e) {
            Throwables.propagate(e);
        }
    }

    private static void killCassandraContainer(String containerName) throws IOException, InterruptedException {
        CONTAINERS.getContainer(containerName).kill();
        DockerPort containerPort = new DockerPort(containerName, CASSANDRA_THRIFT_PORT, CASSANDRA_THRIFT_PORT);
        Awaitility.waitAtMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofSeconds(2))
                .until(() -> !containerPort.isListeningNow());
    }
}
