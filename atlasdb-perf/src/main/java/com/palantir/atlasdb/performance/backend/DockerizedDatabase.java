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
package com.palantir.atlasdb.performance.backend;

import com.palantir.docker.compose.DockerComposeExtension;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

public final class DockerizedDatabase implements Closeable {

    private static final String DOCKER_LOGS_DIR = "container-logs";

    public static DockerizedDatabase start(KeyValueServiceInstrumentation type) {
        DockerComposeExtension docker = DockerComposeExtension.builder()
                .file(type.getDockerComposeResourceFileName())
                .waitingForHostNetworkedPort(type.getKeyValueServicePort(), toBeOpen())
                .saveLogsTo(DOCKER_LOGS_DIR)
                .build();
        InetSocketAddress addr = connect(docker, type.getKeyValueServicePort());
        return new DockerizedDatabase(docker, new DockerizedDatabaseUri(type, addr));
    }

    private static HealthCheck<DockerPort> toBeOpen() {
        return port -> SuccessOrFailure.fromBoolean(port.isListeningNow(), "" + "" + port + " was not open");
    }

    @SuppressWarnings("DnsLookup")
    private static InetSocketAddress connect(DockerComposeExtension docker, int dbPort) {
        try {
            if (docker == null) {
                throw new SafeIllegalStateException("Docker compose extension cannot be run, is null.");
            } else {
                docker.before();
                return new InetSocketAddress(
                        docker.containers().ip(),
                        docker.hostNetworkedPort(dbPort).getExternalPort());
            }
        } catch (IOException | InterruptedException | IllegalStateException e) {
            throw new SafeRuntimeException("Could not run docker compose extension.", e);
        }
    }

    private final DockerComposeExtension docker;
    private final DockerizedDatabaseUri uri;

    private DockerizedDatabase(DockerComposeExtension docker, DockerizedDatabaseUri uri) {
        this.docker = docker;
        this.uri = uri;
    }

    public DockerizedDatabaseUri getUri() {
        return uri;
    }

    @Override
    public void close() {
        if (docker != null) {
            docker.after();
        }
    }
}
