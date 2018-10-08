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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;
import com.palantir.docker.compose.connection.Container;

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
        org.awaitility.Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> {
                    // TODO (jkong): hack
                    String curlOutput = EteSetup.execCliCommand("ete1",
                            String.format("bash -c 'curl %s:%s; echo $?; exit 0;'",
                                    container.getContainerName(), CASSANDRA_PORT));
                    return curlOutput.contains("52");
                });
    }
}
