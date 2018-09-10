/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 * <p>
 * Licensed under the BSD-3 License (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.palantir.cassandra.multinode;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.ClassRule;

import com.google.common.base.Throwables;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.containers.ThreeNodeCassandraCluster;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServiceImpl;
import com.palantir.docker.compose.connection.DockerPort;

public abstract class NodesDownTestSetup {
    private static final int CASSANDRA_THRIFT_PORT = 9160;
    private static final CassandraKeyValueServiceConfig CONFIG = ImmutableCassandraKeyValueServiceConfig
            .copyOf(ThreeNodeCassandraCluster.KVS_CONFIG)
            .withSchemaMutationTimeoutMillis(3_000);

    @ClassRule
    public static final Containers CONTAINERS = new Containers(NodesDownTestSetup.class)
            .with(new ThreeNodeCassandraCluster());

    @AfterClass
    public static void closeKvs() {
        AbstractDegradedClusterTest.closeAll();
    }

    static void initializeKvsAndDegradeCluster(List<Class<?>> tests, List<String> nodesToKill) throws Exception {
        for (Class<?> test : tests) {
            test.getMethod("initialize", CassandraKeyValueService.class).invoke(test.newInstance(), createKvs(test));
        }
        degradeCassandraCluster(nodesToKill);
    }

    private static CassandraKeyValueService createKvs(Class<?> testClass) {
        return CassandraKeyValueServiceImpl
                .createForTesting(getConfig(testClass), ThreeNodeCassandraCluster.LEADER_CONFIG);
    }

    static CassandraKeyValueServiceConfig getConfig(Class<?> testClass) {
        return ImmutableCassandraKeyValueServiceConfig.builder()
                .from(CONFIG)
                .keyspace(testClass.getSimpleName())
                .build();
    }

    private static void degradeCassandraCluster(List<String> nodesToKill) {
        nodesToKill.forEach((containerName) -> {
            try {
                killCassandraContainer(containerName);
            } catch (IOException | InterruptedException e) {
                Throwables.propagate(e);
            }
        });

    }

    private static void killCassandraContainer(String containerName) throws IOException, InterruptedException {
        CONTAINERS.getContainer(containerName).kill();
        DockerPort containerPort = new DockerPort(containerName, CASSANDRA_THRIFT_PORT, CASSANDRA_THRIFT_PORT);
        Awaitility.waitAtMost(10, TimeUnit.SECONDS).pollInterval(2, TimeUnit.SECONDS).until(
                () -> !containerPort.isListeningNow());
    }
}
