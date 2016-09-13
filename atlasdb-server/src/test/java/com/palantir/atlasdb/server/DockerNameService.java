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
package com.palantir.atlasdb.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;
import com.google.common.net.InetAddresses;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.execution.DockerComposeExecutable;
import com.palantir.docker.compose.execution.DockerExecutable;

public class DockerNameService implements sun.net.spi.nameservice.NameService {
    private final Map<String, InetAddress> hostnameMappings;

    public DockerNameService(DockerComposeRule dockerComposeRule) {
        try {
            this.hostnameMappings = Maps.toMap(
                    dockerComposeRule.dockerCompose().ps(),
                    name -> getContainerIpFromName(dockerComposeRule, name));
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public InetAddress[] lookupAllHostAddr(String hostname) throws UnknownHostException {
        if (hostnameMappings.containsKey(hostname)) {
            return new InetAddress[] { hostnameMappings.get(hostname) };
        }
        throw new UnknownHostException(hostname);
    }

    @Override
    public String getHostByAddr(byte[] bytes) throws UnknownHostException {
        throw new UnknownHostException();
    }

    private static InetAddress getContainerIpFromName(DockerComposeRule docker, String containerName) {
        String containerId = getContainerIdFromName(docker.dockerComposeExecutable(), containerName);
        return getContainerIpFromId(docker.dockerExecutable(), containerId);
    }

    private static InetAddress getContainerIpFromId(DockerExecutable docker, String containerId) {
        try {
            Process process = docker.execute(
                    "inspect",
                    "--format",
                    "{{ range .NetworkSettings.Networks }}{{ .IPAddress }}{{ end }}",
                    containerId);
            if (!process.waitFor(5, TimeUnit.SECONDS) || process.exitValue() != 0) {
                throw new IllegalStateException("Couldn't get IP for container ID " + containerId);
            }
            return InetAddresses.forString(getOnlyLineFromInputStream(process.getInputStream()));
        } catch (InterruptedException | IOException e) {
            throw new IllegalStateException("Couldn't get IP for container ID " + containerId, e);
        }
    }

    private static String getContainerIdFromName(DockerComposeExecutable dockerCompose, String containerName) {
        try {
            Process process = dockerCompose.execute("ps", "-q", containerName);
            if (!process.waitFor(5, TimeUnit.SECONDS) || process.exitValue() != 0) {
                throw new IllegalStateException("Couldn't get container ID for container " + containerName);
            }
            return getOnlyLineFromInputStream(process.getInputStream());
        } catch (InterruptedException | IOException e) {
            throw new IllegalStateException("Couldn't get container ID for container " + containerName, e);
        }
    }

    private static String getOnlyLineFromInputStream(InputStream inputStream) throws IOException {
        try (InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
            List<String> lines = CharStreams.readLines(inputStreamReader);
            return Iterables.getOnlyElement(lines);
        }
    }
}
