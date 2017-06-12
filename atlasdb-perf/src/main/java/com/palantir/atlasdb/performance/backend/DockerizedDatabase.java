/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.performance.backend;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.configuration.ShutdownStrategy;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;

public final class DockerizedDatabase implements Closeable {

    private static final String DOCKER_LOGS_DIR = "container-logs";

    public static DockerizedDatabase start(KeyValueServiceInstrumentation type) {
        DockerComposeRule docker = DockerComposeRule.builder()
                .file(getDockerComposeFileAbsolutePath(type.getDockerComposeResourceFileName()))
                .waitingForHostNetworkedPort(type.getKeyValueServicePort(), toBeOpen())
                .saveLogsTo(DOCKER_LOGS_DIR)
                .shutdownStrategy(ShutdownStrategy.AGGRESSIVE_WITH_NETWORK_CLEANUP)
                .build();
        InetSocketAddress addr = connect(docker, type.getKeyValueServicePort());
        return new DockerizedDatabase(docker, new DockerizedDatabaseUri(type, addr));
    }

    private static String getDockerComposeFileAbsolutePath(String dockerComposeResourceFileName) {
        String name = DockerizedDatabase.class.getResource("/" + dockerComposeResourceFileName).getFile();
        return name;
    }

    private static HealthCheck<DockerPort> toBeOpen() {
        return port -> SuccessOrFailure.fromBoolean(port.isListeningNow(), "" + "" + port + " was not open");
    }

    private static InetSocketAddress connect(DockerComposeRule docker, int dbPort) {
        try {
            if (docker == null) {
                throw new IllegalStateException("Docker compose rule cannot be run, is null.");
            } else {
                docker.before();
                return InetSocketAddress.createUnresolved(
                        docker.containers().ip(),
                        docker.hostNetworkedPort(dbPort).getExternalPort());
            }
        } catch (IOException | InterruptedException | IllegalStateException e) {
            throw new RuntimeException("Could not run docker compose rule.", e);
        }
    }

    private final DockerComposeRule docker;
    private final DockerizedDatabaseUri uri;

    private DockerizedDatabase(DockerComposeRule docker, DockerizedDatabaseUri uri) {
        this.docker = docker;
        this.uri = uri;
    }

    public DockerizedDatabaseUri getUri() {
        return uri;
    }

    public void close() {
        if (docker != null) {
            docker.after();
        }
    }
}
