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
package com.palantir.atlasdb.testing;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.CharStreams;
import com.google.common.net.InetAddresses;
import com.palantir.docker.compose.configuration.ProjectName;
import com.palantir.docker.compose.execution.DockerExecutable;

public class ProjectInfo implements Supplier<ProjectInfoMappings> {
    private final DockerExecutable docker;
    private final ProjectName projectName;

    public ProjectInfo(DockerExecutable docker, ProjectName projectName) {
        this.docker = docker;
        this.projectName = projectName;
    }

    public ProjectInfoMappings get() {
        Map<String, InetAddress> hostToIp = getContainerMappings();
        Multimap<String, String> ipToHosts = HashMultimap.create();
        hostToIp.forEach((host, ip) -> ipToHosts.put(ip.getHostAddress(), host));

        return ImmutableProjectInfoMappings.builder()
                .hostToIp(hostToIp)
                .ipToHosts(ipToHosts)
                .build();
    }

    private Map<String, InetAddress> getContainerMappings() {
        ListMultimap<String, String> containerIdToAllIds = getContainerIdToAllIds();
        throwIfDuplicatesInAllIds(containerIdToAllIds);
        return getContainerIdToContainerIp(containerIdToAllIds);
    }

    private ListMultimap<String, String> getContainerIdToAllIds() {
        try {
            Process ps = docker.execute(
                    "ps",
                    "--filter", "label=com.docker.compose.project=" + projectName.constructComposeFileCommand().get(1),
                    "--format", "{{ .ID }},{{ .Names }},{{.Label \"com.docker.compose.service\"}}");
            Preconditions.checkState(ps.waitFor(10, TimeUnit.SECONDS), "'docker ps' timed out after 10 seconds");

            List<String> lines = getLinesFromInputStream(ps.getInputStream());
            List<List<String>> allIds = lines.stream()
                    .map(line -> Splitter.on(',').splitToList(line))
                    .collect(Collectors.toList());
            ListMultimap<String, String> containerIdToAllIds = ArrayListMultimap.create();
            allIds.forEach(ids -> containerIdToAllIds.putAll(Iterables.getFirst(ids, null), ids));
            return containerIdToAllIds;
        } catch (IOException | InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }

    private Map<String, InetAddress> getContainerIdToContainerIp(Multimap<String, String> containerIdToAllIds) {
        ImmutableMap.Builder<String, InetAddress> containerNameToIp = ImmutableMap.builder();

        containerIdToAllIds.asMap().forEach((containerId, ids) -> {
            InetAddress ip = getContainerIpFromId(containerId);
            ids.forEach(id -> containerNameToIp.put(id, ip));
        });

        return containerNameToIp.build();
    }

    private InetAddress getContainerIpFromId(String containerId) {
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

    private static List<String> getLinesFromInputStream(InputStream inputStream) throws IOException {
        try (InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
            return CharStreams.readLines(inputStreamReader);
        }
    }

    private static String getOnlyLineFromInputStream(InputStream inputStream) throws IOException {
        return Iterables.getOnlyElement(getLinesFromInputStream(inputStream));
    }

    private static void throwIfDuplicatesInAllIds(ListMultimap<String, String> containerIdToAllIds) {
        Preconditions.checkState(
                getDuplicateValues(containerIdToAllIds).isEmpty(),
                "Duplicate container IDs/names found: " + getDuplicateValues(containerIdToAllIds));
    }

    private static <T, U> Set<U> getDuplicateValues(ListMultimap<T, U> multimap) {
        List<U> duplicates = new ArrayList<>(multimap.values());
        ImmutableSet.copyOf(multimap.values()).forEach(duplicates::remove);
        return ImmutableSet.copyOf(duplicates);
    }
}
