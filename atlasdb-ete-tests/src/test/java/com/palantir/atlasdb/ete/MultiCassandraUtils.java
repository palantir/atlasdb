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
package com.palantir.atlasdb.ete;

import com.google.common.base.Throwables;
import com.palantir.docker.compose.connection.Container;
import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import org.awaitility.Awaitility;

public final class MultiCassandraUtils {
    public static final int CASSANDRA_THRIFT_PORT = 9160;

    private MultiCassandraUtils() {
        // utility
    }

    /* Kill all of the Cassandra containers and start them again.  This wipes all data on the nodes. */
    public static void resetCassandraCluster(Set<String> containerNames) {
        containerNames.forEach(MultiCassandraUtils::killCassandraContainer);
        containerNames.forEach(MultiCassandraUtils::startCassandraContainer);
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
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> {
                    // TODO (jkong): hack
                    String curlOutput = EteSetup.execCliCommand(
                            "ete1",
                            String.format(
                                    "bash -c 'curl %s:%s; echo $?; exit 0;'",
                                    container.getContainerName(), CASSANDRA_THRIFT_PORT));
                    return curlOutput.contains("52");
                });
    }
}
