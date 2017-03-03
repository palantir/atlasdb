/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.ete;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.joda.time.Duration;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.containers.CassandraVersion;
import com.palantir.atlasdb.testing.DockerProxyRule;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.DockerMachine;
import com.palantir.docker.compose.connection.waiting.ClusterHealthCheck;
import com.palantir.docker.compose.connection.waiting.ClusterWait;
import com.palantir.docker.compose.connection.waiting.HealthChecks;
import com.palantir.docker.compose.execution.DockerComposeExecOption;
import com.palantir.docker.compose.execution.ImmutableDockerComposeExecArgument;
import com.palantir.docker.compose.logging.LogDirectory;

public class DockerClientOrchestrationRule extends ExternalResource {
    public static final String CONTAINER_NAME = "ete1";
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
            updateClientConfig(new File("docker/conf/atlasdb-ete.no-leader.cassandra.yml"));

            DockerMachine dockerMachine = createDockerMachine();

            dockerComposeRule = DockerComposeRule.builder()
                    .machine(dockerMachine)
                    .file("docker-compose.timelock-migration.cassandra.yml")
                    .waitingForService("cassandra", HealthChecks.toHaveAllPortsOpen())
                    .saveLogsTo(LogDirectory.circleAwareLogDirectory(TimeLockMigrationEteTest.class.getSimpleName()))
                    .addClusterWait(new ClusterWait(ClusterHealthCheck.nativeHealthChecks(), Duration.standardMinutes(100)))
                    .build();

            dockerProxyRule = new DockerProxyRule(dockerComposeRule.projectName(), TimeLockMigrationEteTest.class);

            dockerComposeRule.before();
            dockerProxyRule.before();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected void after() {
        dockerProxyRule.after();
        dockerComposeRule.after();
    }

    public void updateProcessLivenessScript() {
        // Go-Java-Launcher's functionality for seeing if a process is running is not correct with respect to busybox.
        // See also https://github.com/palantir/sls-packaging/issues/185
        try {
            dockerComposeRule.exec(
                    DockerComposeExecOption.noOptions(),
                    CONTAINER_NAME,
                    ImmutableDockerComposeExecArgument.arguments(
                            "sed", "-i", "s/ps $PID > \\/dev\\/null;/kill -0 $PID/", "service/bin/init.sh"));
        } catch (IOException | InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }

    public void updateClientConfig(File file) {
        try {
            FileUtils.writeStringToFile(configFile, FileUtils.readFileToString(file));
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public void restartAtlasClient() throws IOException, InterruptedException {
        dockerComposeRule.exec(
                DockerComposeExecOption.noOptions(),
                CONTAINER_NAME,
                ImmutableDockerComposeExecArgument.arguments(
                        "bash", "-c", "nohup service/bin/init.sh restart"));
    }

    public String getClientLogs() throws IOException, InterruptedException {
        return dockerComposeRule.exec(
                DockerComposeExecOption.noOptions(),
                CONTAINER_NAME,
                ImmutableDockerComposeExecArgument.arguments(
                        "cat", "var/log/atlasdb-ete-startup.log"));
    }


    private DockerMachine createDockerMachine() {
        Map<String, String> environment = getEnvironment();
        return DockerMachine.localMachine()
                .withEnvironment(environment)
                .build();
    }

    private Map<String, String> getEnvironment() {
        Map<String, String> environment = Maps.newHashMap(CassandraVersion.getEnvironment());
        environment.put("CONFIG_FILE_MOUNTPOINT", temporaryFolder.getRoot().getAbsolutePath());
        return environment;
    }

}
