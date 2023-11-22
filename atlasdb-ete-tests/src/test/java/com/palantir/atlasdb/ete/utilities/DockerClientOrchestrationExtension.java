/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.ete.utilities;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.containers.CassandraEnvironment;
import com.palantir.atlasdb.ete.standalone.TimeLockMigrationEteTest;
import com.palantir.docker.compose.DockerComposeExtension;
import com.palantir.docker.compose.connection.DockerMachine;
import com.palantir.docker.compose.connection.waiting.ClusterHealthCheck;
import com.palantir.docker.compose.connection.waiting.ClusterWait;
import com.palantir.docker.compose.connection.waiting.HealthChecks;
import com.palantir.docker.compose.execution.DockerComposeExecOption;
import com.palantir.docker.compose.execution.DockerExecutionException;
import com.palantir.docker.compose.execution.ImmutableDockerComposeExecArgument;
import com.palantir.docker.compose.logging.LogDirectory;
import com.palantir.docker.proxy.DockerProxyExtension;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.test.utils.SubdirectoryCreator;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.immutables.value.Value;
import org.joda.time.Duration;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class DockerClientOrchestrationExtension implements BeforeAllCallback, AfterAllCallback {
    private static final SafeLogger log = SafeLoggerFactory.get(DockerClientOrchestrationExtension.class);

    private static final String CONTAINER = "ete1";
    private static final Duration WAIT_TIMEOUT = Duration.standardMinutes(5);
    private static final int MAX_EXEC_TRIES = 10;

    private final DockerClientConfigurationV2 clientConfiguration;
    private final File temporaryFolder;

    private DockerComposeExtension dockerComposeExtension;
    private DockerProxyExtension dockerProxyExtension;
    private File configFile;

    public DockerClientOrchestrationExtension(
            DockerClientConfigurationV2 dockerClientConfiguration, File temporaryFolder) {
        this.clientConfiguration = dockerClientConfiguration;
        this.temporaryFolder = temporaryFolder;
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        try {
            configFile = SubdirectoryCreator.createAndGetFile(temporaryFolder, "atlasdb-ete.yml");
            updateClientConfig(clientConfiguration.initialConfigFile());
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        DockerMachine dockerMachine = createDockerMachine();
        dockerComposeExtension = DockerComposeExtension.builder()
                .machine(dockerMachine)
                .file(clientConfiguration.dockerComposeYmlFile().toString())
                .waitingForService(clientConfiguration.databaseServiceName(), HealthChecks.toHaveAllPortsOpen())
                .saveLogsTo(LogDirectory.circleAwareLogDirectory(TimeLockMigrationEteTest.class.getSimpleName()))
                .addClusterWait(new ClusterWait(ClusterHealthCheck.nativeHealthChecks(), WAIT_TIMEOUT))
                .build();
        dockerProxyExtension = DockerProxyExtension.fromProjectName(
                dockerComposeExtension.projectName(), TimeLockMigrationEteTest.class);

        dockerComposeExtension.beforeAll(extensionContext);
        dockerProxyExtension.beforeAll(extensionContext);
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) {
        dockerProxyExtension.afterAll(extensionContext);
        dockerComposeExtension.afterAll(extensionContext);
    }

    public void updateProcessLivenessScript() {
        // Go-Java-Launcher's functionality for seeing if a process is running is not correct with respect to busybox.
        // See also https://github.com/palantir/sls-packaging/issues/185
        dockerExecOnClient("sed", "-i", "s/ps $PID > \\/dev\\/null;/kill -0 $PID/", "service/bin/init.sh");
    }

    public void updateClientConfig(File file) {
        try {
            FileUtils.writeStringToFile(configFile, FileUtils.readFileToString(file));
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public void restartAtlasClient() {
        runInitShWithVerb("restart");
    }

    public void stopAtlasClient() {
        runInitShWithVerb("stop");
    }

    public String getClientLogs() {
        return dockerExecOnClient("cat", "var/log/startup.log");
    }

    private void runInitShWithVerb(String verb) {
        // Need nohup - otherwise our process is a child of our shell, and will be killed when we're done.
        dockerExecOnClient("bash", "-c", "nohup service/bin/init.sh " + verb);
    }

    private DockerMachine createDockerMachine() {
        return DockerMachine.localMachine().withEnvironment(getEnvironment()).build();
    }

    private Map<String, String> getEnvironment() {
        return ImmutableMap.<String, String>builder()
                .putAll(CassandraEnvironment.get())
                .put("CONFIG_FILE_MOUNTPOINT", temporaryFolder.getAbsolutePath())
                .buildOrThrow();
    }

    private String dockerExecOnClient(String... arguments) {
        for (int i = 1; i <= MAX_EXEC_TRIES; i++) {
            try {
                log.info(
                        "Attempting docker-exec with arguments: {}",
                        UnsafeArg.of("arguments", Arrays.asList(arguments)));
                return dockerComposeExtension.exec(
                        DockerComposeExecOption.noOptions(),
                        CONTAINER,
                        ImmutableDockerComposeExecArgument.arguments(arguments));
            } catch (InterruptedException | IOException e) {
                throw Throwables.propagate(e);
            } catch (DockerExecutionException e) {
                if (i != MAX_EXEC_TRIES) {
                    // I have seen very odd flakes where exec terminates with exit code 129
                    // i.e. they are interrupted with the hangup signal.
                    log.warn(
                            "Encountered error in docker-exec, retrying (attempt {} of {})",
                            SafeArg.of("attempt", i),
                            SafeArg.of("maxAttempts", MAX_EXEC_TRIES),
                            e);
                } else {
                    log.error("Made {} attempts, and now giving up", SafeArg.of("maxAttempts", MAX_EXEC_TRIES), e);
                    throw e;
                }
            }
        }
        throw new IllegalStateException(
                String.format("Unexpected state after %s unsuccessful attempts in docker-exec", MAX_EXEC_TRIES));
    }

    @Value.Immutable
    public interface DockerClientConfigurationV2 {
        File dockerComposeYmlFile();

        File initialConfigFile();

        String databaseServiceName();
    }
}
