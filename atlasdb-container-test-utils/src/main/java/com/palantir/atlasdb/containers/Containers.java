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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.palantir.docker.compose.DockerComposeExtension;
import com.palantir.docker.compose.configuration.DockerComposeFiles;
import com.palantir.docker.compose.configuration.ProjectName;
import com.palantir.docker.compose.configuration.ShutdownStrategy;
import com.palantir.docker.compose.connection.DockerMachine;
import com.palantir.docker.compose.execution.DockerCompose;
import com.palantir.docker.compose.logging.LogDirectory;
import com.palantir.docker.proxy.DockerProxyExtension;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

@SuppressWarnings("ShutdownHook")
public class Containers implements BeforeAllCallback, AfterAllCallback {
    private static final ProjectName PROJECT_NAME = ProjectName.fromString("atlasdbcontainers");

    @VisibleForTesting
    static final DockerProxyExtension DOCKER_PROXY_EXTENSION =
            DockerProxyExtension.fromProjectName(PROJECT_NAME, Container.class);

    private static final Set<Container> containersToStart = new HashSet<>();
    private static final Set<Container> containersStarted = new HashSet<>();
    private static final LoadingCache<String, String> dockerComposeFilesToTemporaryCopies =
            Caffeine.newBuilder().build(Containers::getDockerComposeFile);

    private static volatile DockerComposeExtension dockerComposeExtension;
    private static volatile InterruptibleFileLogCollector currentLogCollector;
    private static volatile boolean shutdownHookAdded;
    private static volatile boolean dockerProxyExtensionStarted;

    private final String logDirectory;

    public Containers(Class<?> classToSaveLogsFor) {
        this.logDirectory =
                LogDirectory.circleAwareLogDirectory(Paths.get("atlasdbcontainers", classToSaveLogsFor.getSimpleName())
                        .toString());
    }

    public Containers with(Container container) {
        synchronized (Containers.class) {
            containersToStart.add(container);
        }
        return this;
    }

    public com.palantir.docker.compose.connection.Container getContainer(String containerName) {
        synchronized (Containers.class) {
            return dockerComposeExtension.containers().container(containerName);
        }
    }

    public DockerCompose getDockerCompose() {
        return dockerComposeExtension.dockerCompose();
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws IOException, InterruptedException {
        synchronized (Containers.class) {
            setupShutdownHook();

            setupLogCollectorForLogDirectory();
            setupDockerComposeExtension();
            startCollectingLogs();

            ensureDockerProxyExtensionRunning();

            waitForContainersToStart();
            containersStarted.addAll(containersToStart);
        }
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) {
        afterAll();
    }

    public void afterAll() {
        currentLogCollector.stopExecutor();
    }

    public String getLogDirectory() {
        return logDirectory;
    }

    public static Proxy getSocksProxy(String name) {
        try {
            return ProxySelector.getDefault().select(new URI("tcp", name, null, null)).stream()
                    .filter(proxy -> proxy.type() == Proxy.Type.SOCKS)
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException("Socks proxy has to exist"));
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

    private void setupDockerComposeExtension() throws InterruptedException, IOException {
        DockerComposeFiles files = DockerComposeFiles.from(containersToStart.stream()
                .map(Container::getDockerComposeFile)
                .map(dockerComposeFilesToTemporaryCopies::get)
                .distinct()
                .toArray(String[]::new));

        Map<String, String> environment = containersToStart.stream()
                .flatMap(container -> container.getEnvironment().entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (fst, snd) -> snd));

        DockerMachine machine =
                DockerMachine.localMachine().withEnvironment(environment).build();

        dockerComposeExtension = DockerComposeExtension.builder()
                .files(files)
                .projectName(PROJECT_NAME)
                .machine(machine)
                .logCollector(currentLogCollector)
                .shutdownStrategy(ShutdownStrategy.AGGRESSIVE_WITH_NETWORK_CLEANUP)
                .build();

        dockerComposeExtension.before();
    }

    private void startCollectingLogs() {
        currentLogCollector.initializeExecutor(
                Sets.union(containersToStart, containersStarted).size());
    }

    private static void setupShutdownHook() {
        if (!shutdownHookAdded) {
            shutdownHookAdded = true;
            Runtime.getRuntime().addShutdownHook(new Thread(Containers::onShutdown));
        }
    }

    @SuppressWarnings("checkstyle:IllegalThrows")
    private static void ensureDockerProxyExtensionRunning() throws InterruptedException, IOException {
        if (!dockerProxyExtensionStarted) {
            dockerProxyExtensionStarted = true;
            DOCKER_PROXY_EXTENSION.before();
        }
    }

    private static void waitForContainersToStart() {
        for (Container container : Sets.difference(containersToStart, containersStarted)) {
            Awaitility.await()
                    .atMost(Duration.ofMinutes(5))
                    .pollInterval(Duration.ofSeconds(1))
                    .until(() -> container.isReady(dockerComposeExtension).succeeded());
        }
    }

    private static String getDockerComposeFile(String configResource) {
        try {
            File configFile = File.createTempFile("config", ".yml");
            Containers.class.getResourceAsStream(configResource).transferTo(new FileOutputStream(configFile));
            return configFile.getPath();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @VisibleForTesting
    static void onShutdown() {
        synchronized (Containers.class) {
            if (dockerProxyExtensionStarted) {
                DOCKER_PROXY_EXTENSION.after();
            }
            if (dockerComposeExtension != null) {
                dockerComposeExtension.after();
            }
            dockerProxyExtensionStarted = false;
            dockerComposeExtension = null;
            currentLogCollector = null;
            containersStarted.clear();
        }
    }
}
