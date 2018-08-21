/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;
import com.jayway.awaitility.Awaitility;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;

public final class MultiCassandraUtils {
    private static final int CASSANDRA_PORT = 9160;

    private MultiCassandraUtils() {
        // utility
    }

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
        DockerPort containerPort = new DockerPort(container.getContainerName(), CASSANDRA_PORT, CASSANDRA_PORT);
        Awaitility.await()
                .atMost(180, TimeUnit.SECONDS)
                .until(containerPort::isListeningNow);
    }
}
