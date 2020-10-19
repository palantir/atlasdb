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
package com.palantir.atlasdb.containers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.configuration.DockerComposeFiles;
import com.palantir.docker.compose.configuration.ProjectName;
import com.palantir.docker.compose.configuration.ShutdownStrategy;
import com.palantir.docker.compose.connection.DockerMachine;
import com.palantir.docker.compose.execution.DockerCompose;
import com.palantir.docker.compose.logging.LogDirectory;
import com.palantir.docker.proxy.DockerProxyRule;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.rules.ExternalResource;

@SuppressWarnings("ShutdownHook")
public class Containers extends ExternalResource {
    private static final ProjectName PROJECT_NAME = ProjectName.fromString("atlasdbcontainers");
    @VisibleForTesting
    static final DockerProxyRule DOCKER_PROXY_RULE = DockerProxyRule.fromProjectName(PROJECT_NAME, Container.class);

    private static final Set<Container> containersToStart = new HashSet<>();
    private static final Set<Container> containersStarted = new HashSet<>();
    private static final LoadingCache<String, String> dockerComposeFilesToTemporaryCopies = CacheBuilder.newBuilder()
                    .build(CacheLoader.from(Containers::getDockerComposeFile));

    private static volatile DockerComposeRule dockerComposeRule;
    private static volatile InterruptibleFileLogCollector currentLogCollector;
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

    public DockerCompose getDockerCompose() {
        return dockerComposeRule.dockerCompose();
    }

    @Override
    public void before() throws Throwable {
        synchronized (Containers.class) {
            setupShutdownHook();

            setupLogCollectorForLogDirectory();
            setupDockerComposeRule();
            startCollectingLogs();

            ensureDockerProxyRuleRunning();

            waitForContainersToStart();
            containersStarted.addAll(containersToStart);
        }
    }

    @Override
    protected void after() {
        currentLogCollector.stopExecutor();
    }

    public String getLogDirectory() {
        return logDirectory;
    }

    public static Proxy getSocksProxy(String name) {
        try {
            return ProxySelector.getDefault()
                    .select(new URI("tcp", name, null, null))
                    .stream()
                    .filter(proxy -> proxy.type() == Proxy.Type.SOCKS)
                    .findFirst()
                    .orElseThrow(() ->  new RuntimeException("Socks proxy has to exist"));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private void setupLogCollectorForLogDirectory() {
        if (currentLogCollector != null) {
            currentLogCollector.stopExecutor();
        }
        currentLogCollector = new InterruptibleFileLogCollector(new File(logDirectory));
    }

    private void setupDockerComposeRule() throws InterruptedException, IOException {
        Set<String> containerDockerComposeFiles = containersToStart.stream()
                .map(Container::getDockerComposeFile)
                .map(dockerComposeFilesToTemporaryCopies::getUnchecked)
                .collect(Collectors.toSet());

        Map<String, String> environment = containersToStart.stream()
                .flatMap(container -> container.getEnvironment().entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (fst, snd) -> snd));

        DockerMachine machine = DockerMachine.localMachine()
                .withEnvironment(environment)
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

    private void startCollectingLogs() {
        currentLogCollector.initializeExecutor(Sets.union(containersToStart, containersStarted).size());
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
