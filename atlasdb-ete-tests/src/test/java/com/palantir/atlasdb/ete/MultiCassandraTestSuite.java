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
package com.palantir.atlasdb.ete;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.jayway.awaitility.Awaitility;
import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.CassandraVersion;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        TodoEteTest.class,
        DropwizardEteTest.class,
        MultiCassandraSingleNodeDownEteTest.class,
        MultiCassandraDoubleNodeDownEteTest.class
        })
public class MultiCassandraTestSuite extends EteSetup {
    private static final List<String> CLIENTS = ImmutableList.of("ete1");

    @ClassRule
    public static final RuleChain COMPOSITION_SETUP = EteSetup.setupComposition(
            MultiCassandraTestSuite.class,
            "docker-compose.multiple-cassandra.yml",
            CLIENTS,
            CassandraVersion.getEnvironment());

    public static void killCassandraContainer(String containerName) {
        Container container = EteSetup.getContainer(containerName);
        try {
            container.kill();
        } catch (IOException | InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }

    public static void startCassandraContainer(String containerName) {
        Container container = EteSetup.getContainer(containerName);
        try {
            container.start();
        } catch (IOException | InterruptedException e) {
            throw Throwables.propagate(e);
        }
        waitForCassandraContainer(container);
    }

    private static void waitForCassandraContainer(Container container) {
        DockerPort thriftPort = new DockerPort(
                container.getContainerName(),
                CassandraContainer.THRIFT_PORT,
                CassandraContainer.THRIFT_PORT);
        DockerPort cqlPort = new DockerPort(
                container.getContainerName(),
                CassandraContainer.CQL_PORT,
                CassandraContainer.CQL_PORT);

        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .until(() -> thriftPort.isListeningNow() && cqlPort.isListeningNow());
    }
}
