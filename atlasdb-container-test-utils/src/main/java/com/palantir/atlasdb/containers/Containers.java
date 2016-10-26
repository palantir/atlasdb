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
package com.palantir.atlasdb.containers;

import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.rules.ExternalResource;

import com.google.common.collect.Sets;
import com.jayway.awaitility.Awaitility;
import com.palantir.atlasdb.testing.DockerProxyRule;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.configuration.DockerComposeFiles;
import com.palantir.docker.compose.configuration.ProjectName;
import com.palantir.docker.compose.logging.LogCollector;
import com.palantir.docker.compose.logging.LogDirectory;

public class Containers extends ExternalResource {
    private static final ProjectName PROJECT_NAME = ProjectName.fromString("atlasdbcontainers");
    private static final DockerProxyRule DOCKER_PROXY_RULE = new DockerProxyRule(PROJECT_NAME, Container.class);

    private static final Set<Container> containersToStart = new HashSet<>();
    private static final Set<Container> containersStarted = new HashSet<>();

    private static volatile DockerComposeRule dockerComposeRule;
    private static volatile LogCollector currentLogCollector;
    private static volatile boolean shutdownHookAdded;
    private static volatile boolean dockerProxyRuleStarted;

    private final String logDirectory;

    public Containers(Class<?> classToSaveLogsFor) {
        logDirectory = LogDirectory.circleAwareLogDirectory(Paths.get(
                "atlasdbcontainers",
                classToSaveLogsFor.getSimpleName()).toString());
    }

    public Containers with(Container container) {
        synchronized (Containers.class) {
            containersToStart.add(container);
        }
        return this;
    }

    @Override
    protected void before() throws Throwable {
        synchronized (Containers.class) {
            if (!shutdownHookAdded) {
                shutdownHookAdded = true;
                Runtime.getRuntime().addShutdownHook(new Thread(Containers::onShutdown));
            }

            Set<String> containerDockerComposeFiles = containersToStart.stream()
                    .map(Container::getDockerComposeFile)
                    .map(file -> Container.class.getResource(file).getPath())
                    .collect(Collectors.toSet());

            if (currentLogCollector != null) {
                currentLogCollector.stopCollecting();
            }
            currentLogCollector = InterruptibleFileLogCollector.fromPath(logDirectory);

            dockerComposeRule = DockerComposeRule.builder()
                    .files(DockerComposeFiles.from(containerDockerComposeFiles.toArray(new String[0])))
                    .projectName(PROJECT_NAME)
                    .logCollector(currentLogCollector)
                    .build();
            dockerComposeRule.before();

            if (!dockerProxyRuleStarted) {
                dockerProxyRuleStarted = true;
                DOCKER_PROXY_RULE.before();
            }

            for (Container container : Sets.difference(containersToStart, containersStarted)) {
                Awaitility.await()
                        .atMost(com.jayway.awaitility.Duration.ONE_MINUTE)
                        .pollInterval(com.jayway.awaitility.Duration.ONE_SECOND)
                        .until(() -> container.isReady().succeeded());
            }
            containersStarted.addAll(containersToStart);
        }
    }

    private static void onShutdown() {
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

    public String getLogDirectory() {
        return logDirectory;
    }
}
