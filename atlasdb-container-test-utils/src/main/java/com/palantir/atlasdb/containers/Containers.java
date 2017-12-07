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
package com.palantir.atlasdb.containers;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.rules.ExternalResource;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.configuration.DockerComposeFiles;
import com.palantir.docker.compose.configuration.ProjectName;
import com.palantir.docker.compose.configuration.ShutdownStrategy;
import com.palantir.docker.compose.connection.DockerMachine;
import com.palantir.docker.compose.logging.LogCollector;
import com.palantir.docker.compose.logging.LogDirectory;
import com.palantir.docker.proxy.DockerProxyRule;

public class Containers extends ExternalResource {
    private static final ProjectName PROJECT_NAME = ProjectName.fromString("atlasdbcontainers");
    @VisibleForTesting
    static final DockerProxyRule DOCKER_PROXY_RULE = DockerProxyRule.fromProjectName(PROJECT_NAME, Container.class);

    private static final Set<Container> containersToStart = new HashSet<>();
    private static final Set<Container> containersStarted = new HashSet<>();
    private static final LoadingCache<String, String> dockerComposeFilesToTemporaryCopies = CacheBuilder.newBuilder()
                    .build(CacheLoader.from(Containers::getDockerComposeFile));

    private static volatile DockerComposeRule dockerComposeRule;
    private static volatile LogCollector currentLogCollector;
    private static volatile boolean shutdownHookAdded;
    private static volatile boolean dockerProxyRuleStarted;

    private final String logDirectory;

    public Containers(Class<?> classToSaveLogsFor) {
        this.logDirectory = LogDirectory.circleAwareLogDirectory(Paths.get(
                "atlasdbcontainers",
                classToSaveLogsFor.getSimpleName()).toString());
    }

    public Containers with(Container container) {
        synchronized (Containers.class) {
            containersToStart.add(container);
        }
        return this;
    }

    public com.palantir.docker.compose.connection.Container getContainer(String containerName) {
        synchronized (Containers.class) {
            return dockerComposeRule.containers().container(containerName);
        }
    }

    @Override
    protected void before() throws Throwable {
        synchronized (Containers.class) {
            setupShutdownHook();

            setupLogCollectorForLogDirectory();
            setupDockerComposeRule();

            ensureDockerProxyRuleRunning();

            waitForContainersToStart();
            containersStarted.addAll(containersToStart);
        }
    }

    public String getLogDirectory() {
        return logDirectory;
    }

    private void setupLogCollectorForLogDirectory() throws InterruptedException {
        if (currentLogCollector != null) {
            currentLogCollector.stopCollecting();
        }
        currentLogCollector = InterruptibleFileLogCollector.fromPath(logDirectory);
    }

    private void setupDockerComposeRule() throws InterruptedException, IOException {
        Set<String> containerDockerComposeFiles = containersToStart.stream()
                .map(Container::getDockerComposeFile)
                .map(dockerComposeFilesToTemporaryCopies::getUnchecked)
                .collect(Collectors.toSet());

        ImmutableMap.Builder<String, String> environment = ImmutableMap.builder();
        containersToStart.forEach(c -> environment.putAll(c.getEnvironment()));

        DockerMachine machine = DockerMachine.localMachine()
                .withEnvironment(environment.build())
                .build();

        dockerComposeRule = DockerComposeRule.builder()
                .files(DockerComposeFiles.from(containerDockerComposeFiles.toArray(new String[0])))
                .projectName(PROJECT_NAME)
                .machine(machine)
                .logCollector(currentLogCollector)
                .shutdownStrategy(ShutdownStrategy.AGGRESSIVE_WITH_NETWORK_CLEANUP)
                .build();

        dockerComposeRule.before();
    }

    private static void setupShutdownHook() {
        if (!shutdownHookAdded) {
            shutdownHookAdded = true;
            Runtime.getRuntime().addShutdownHook(new Thread(Containers::onShutdown));
        }
    }

    @SuppressWarnings("checkstyle:IllegalThrows")
    private static void ensureDockerProxyRuleRunning() throws Throwable {
        if (!dockerProxyRuleStarted) {
            dockerProxyRuleStarted = true;
            DOCKER_PROXY_RULE.before();
        }
    }

    private static void waitForContainersToStart() {
        for (Container container : Sets.difference(containersToStart, containersStarted)) {
            Awaitility.await()
                    .atMost(Duration.FIVE_MINUTES)
                    .pollInterval(Duration.ONE_SECOND)
                    .until(() -> container.isReady(dockerComposeRule).succeeded());
        }
    }

    private static String getDockerComposeFile(String configResource) {
        try {
            File configFile = File.createTempFile("config", ".yml");
            IOUtils.copy(Containers.class.getResourceAsStream(configResource), new FileOutputStream(configFile));
            return configFile.getPath();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @VisibleForTesting
    static void onShutdown() {
        synchronized (Containers.class) {
            if (dockerProxyRuleStarted) {
                DOCKER_PROXY_RULE.after();
            }
            if (dockerComposeRule != null) {
                dockerComposeRule.after();
            }
            dockerProxyRuleStarted = false;
            dockerComposeRule = null;
            currentLogCollector = null;
            containersStarted.clear();
        }
    }
}
