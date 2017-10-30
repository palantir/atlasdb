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

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.execution.DockerComposeRunArgument;
import com.palantir.docker.compose.execution.DockerComposeRunOption;

public class CassandraClusterOperations {
    private static final Logger log = LoggerFactory.getLogger(CassandraClusterOperations.class);
    private static final int NODETOOL_DISABLEAUTOCOMPACTION_TIMEOUT_SECONDS = 10;
    private static final int NODETOOL_STATUS_TIMEOUT_SECONDS = 10;
    private static final int NODETOOL_REPAIR_TIMEOUT_SECONDS = 60;

    private final DockerComposeRule dockerComposeRule;
    private final CassandraCliParser cassandraCliParser;
    private final CassandraVersion cassandraVersion;
    private final List<String> containerNames;
    private final String cliContainerName;

    public CassandraClusterOperations(DockerComposeRule dockerComposeRule,
            CassandraVersion version,
            List<String> containerNames,
            String cliContainerName) {
        Preconditions.checkArgument(!containerNames.isEmpty(), "Must have at least one node!");
        this.dockerComposeRule = dockerComposeRule;
        this.cassandraVersion = version;
        this.cassandraCliParser = new CassandraCliParser(version);
        this.containerNames = containerNames;
        this.cliContainerName = cliContainerName;
    }

    public boolean nodetoolShowsAllCassandraNodesUp() {
        try {
            String output = runNodetoolCommand("status", NODETOOL_STATUS_TIMEOUT_SECONDS);
            int numberNodesUp = cassandraCliParser.parseNumberOfUpNodesFromNodetoolStatus(output);
            return numberNodesUp == containerNames.size();
        } catch (Exception e) {
            log.warn("Failed while running nodetool status", e);
            return false;
        }
    }

    public void replicateSystemAuthenticationDataOnAllNodes()
            throws IOException, InterruptedException {
        if (!systemAuthenticationKeyspaceHasReplicationFactorThree()) {
            setReplicationFactorOfSystemAuthenticationKeyspaceToThree();
            runNodetoolRepair();
        }
    }

    public void disableAutoCompaction() throws IOException, InterruptedException {
        runNodetoolCommandOnAllContainers("disableautocompaction", NODETOOL_DISABLEAUTOCOMPACTION_TIMEOUT_SECONDS);
    }

    private void runNodetoolRepair() throws IOException, InterruptedException {
        runNodetoolCommand("repair system_auth", NODETOOL_REPAIR_TIMEOUT_SECONDS);
    }

    private void setReplicationFactorOfSystemAuthenticationKeyspaceToThree()
            throws IOException, InterruptedException {
        runCql("ALTER KEYSPACE system_auth "
                + "WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 3};");
    }

    private boolean systemAuthenticationKeyspaceHasReplicationFactorThree()
            throws IOException, InterruptedException {
        String getAllKeyspaces = cassandraVersion.getAllKeyspacesCql();
        String output = runCql(getAllKeyspaces);
        int replicationFactor = cassandraCliParser.parseSystemAuthReplicationFromCqlsh(output);
        return replicationFactor == 3;
    }

    private String runCql(String cql) throws IOException, InterruptedException {
        return runCommandInCliContainer(
                "cqlsh",
                "--username", CassandraContainer.USERNAME,
                "--password", CassandraContainer.PASSWORD,
                containerNames.get(0),
                "--execute", cql);
    }

    private String runNodetoolCommand(String nodetoolCommand, int timeoutSeconds) throws IOException,
            InterruptedException {
        return runNodetoolCommandOnContainer(nodetoolCommand, timeoutSeconds, containerNames.get(0));
    }

    private List<String> runNodetoolCommandOnAllContainers(String nodetoolCommand, int timeoutSeconds)
            throws IOException, InterruptedException {
        List<String> ret = Lists.newArrayList();
        for (String containerName : containerNames) {
            ret.add(runNodetoolCommandOnContainer(nodetoolCommand, timeoutSeconds, containerName));
        }
        return ret;
    }

    private String runNodetoolCommandOnContainer(String nodetoolCommand, int timeoutSeconds, String container)
            throws IOException, InterruptedException {
        return runCommandInCliContainer(
                "timeout",
                Integer.toString(timeoutSeconds),
                "nodetool",
                "--host", container,
                nodetoolCommand);
    }

    private String runCommandInCliContainer(String... arguments) throws IOException,
            InterruptedException {
        return dockerComposeRule.run(
                DockerComposeRunOption.options("-T"),
                cliContainerName,
                DockerComposeRunArgument.arguments(arguments));
    }
}
