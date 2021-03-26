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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.containers.CassandraEnvironment;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.DockerMachine;
import com.palantir.docker.compose.connection.waiting.ClusterHealthCheck;
import com.palantir.docker.compose.connection.waiting.ClusterWait;
import com.palantir.docker.compose.connection.waiting.HealthChecks;
import com.palantir.docker.compose.execution.DockerComposeExecOption;
import com.palantir.docker.compose.execution.DockerExecutionException;
import com.palantir.docker.compose.execution.ImmutableDockerComposeExecArgument;
import com.palantir.docker.compose.logging.LogDirectory;
import com.palantir.docker.proxy.DockerProxyRule;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.joda.time.Duration;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DockerClientOrchestrationRule extends ExternalResource {
    private static final Logger log = LoggerFactory.getLogger(DockerClientOrchestrationRule.class);

    public static final File TIMELOCK_CONFIG = new File("docker/conf/atlasdb-ete.timelock.dbkvs.yml");
    public static final File EMBEDDED_CONFIG = new File("docker/conf/atlasdb-ete.no-leader.dbkvs.yml");

    private static final String CONTAINER = "ete1.palantir.pt";
    private static final Duration WAIT_TIMEOUT = Duration.standardMinutes(5);
    private static final int MAX_EXEC_TRIES = 10;

    private final TemporaryFolder temporaryFolder;

    private DockerComposeRule dockerComposeRule;
    private DockerProxyRule dockerProxyRule;
    private File configFile;

    public DockerClientOrchestrationRule(TemporaryFolder temporaryFolder) {
        this.temporaryFolder = temporaryFolder;
    }

    @Override
    protected void before() throws Throwable {
        try {
            configFile = temporaryFolder.newFile("atlasdb-ete.yml");
            updateClientConfig(EMBEDDED_CONFIG);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        DockerMachine dockerMachine = createDockerMachine();
        dockerComposeRule = DockerComposeRule.builder()
                .machine(dockerMachine)
                .file("docker-compose.timelock.database.bound.postgres.yml")
                .waitingForService("postgres", HealthChecks.toHaveAllPortsOpen())
                .saveLogsTo(LogDirectory.circleAwareLogDirectory(TimeLockMigrationEteTest.class.getSimpleName()))
                .addClusterWait(new ClusterWait(ClusterHealthCheck.nativeHealthChecks(), WAIT_TIMEOUT))
                .build();
        dockerProxyRule =
                DockerProxyRule.fromProjectName(dockerComposeRule.projectName(), TimeLockMigrationEteTest.class);

        dockerComposeRule.before();
        dockerProxyRule.before();
    }

    @Override
    protected void after() {
        dockerProxyRule.after();
        dockerComposeRule.after();
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
        // Need nohup - otherwise our process is a child of our shell, and will be killed when we're done.
        dockerExecOnClient("bash", "-c", "nohup service/bin/init.sh restart");
    }

    public String getClientLogs() {
        return dockerExecOnClient("cat", "var/log/atlasdb-ete-startup.log");
    }

    private DockerMachine createDockerMachine() {
        return DockerMachine.localMachine().withEnvironment(getEnvironment()).build();
    }

    private Map<String, String> getEnvironment() {
        return ImmutableMap.<String, String>builder()
                .putAll(CassandraEnvironment.get())
                .put("CONFIG_FILE_MOUNTPOINT", temporaryFolder.getRoot().getAbsolutePath())
                .build();
    }

    private String dockerExecOnClient(String... arguments) {
        for (int i = 1; i <= MAX_EXEC_TRIES; i++) {
            try {
                log.info("Attempting docker-exec with arguments: {}", Arrays.asList(arguments));
                return dockerComposeRule.exec(
                        DockerComposeExecOption.noOptions(),
                        CONTAINER,
                        ImmutableDockerComposeExecArgument.arguments(arguments));
            } catch (InterruptedException | IOException e) {
                throw Throwables.propagate(e);
            } catch (DockerExecutionException e) {
                if (i != MAX_EXEC_TRIES) {
                    // I have seen very odd flakes where exec terminates with exit code 129
                    // i.e. they are interrupted with the hangup signal.
                    log.warn("Encountered error in docker-exec, retrying (attempt {} of {})", i, MAX_EXEC_TRIES, e);
                } else {
                    log.error("Made {} attempts, and now giving up", MAX_EXEC_TRIES, e);
                    throw e;
                }
            }
        }
        throw new IllegalStateException(
                String.format("Unexpected state after %s unsuccessful attempts in docker-exec", MAX_EXEC_TRIES));
    }
}
