/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.execution.DockerComposeRunArgument;
import com.palantir.docker.compose.execution.DockerComposeRunOption;

public class ThreeNodeCassandraClusterOperations {
    private static final Logger log = LoggerFactory.getLogger(ThreeNodeCassandraClusterOperations.class);
    private static final int NODETOOL_STATUS_TIMEOUT_SECONDS = 10;
    private static final int NODETOOL_REPAIR_TIMEOUT_SECONDS = 60;

    private final DockerComposeRule dockerComposeRule;
    private final CassandraCliParser cassandraCliParser;
    private final CassandraVersion cassandraVersion;

    public ThreeNodeCassandraClusterOperations(DockerComposeRule dockerComposeRule, CassandraVersion version) {
        this.dockerComposeRule = dockerComposeRule;
        this.cassandraVersion = version;
        this.cassandraCliParser = new CassandraCliParser(version);
    }

    public boolean nodetoolShowsThreeCassandraNodesUp() {
        try {
            String output = runNodetoolCommand("status", NODETOOL_STATUS_TIMEOUT_SECONDS);
            int numberNodesUp = cassandraCliParser.parseNumberOfUpNodesFromNodetoolStatus(output);
            return numberNodesUp == 3;
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
                ThreeNodeCassandraCluster.FIRST_CASSANDRA_CONTAINER_NAME,
                "--execute", cql);
    }

    private String runNodetoolCommand(String nodetoolCommand, int timeoutSeconds) throws IOException,
            InterruptedException {
        return runCommandInCliContainer(
                "timeout",
                Integer.toString(timeoutSeconds),
                "nodetool",
                "--host", ThreeNodeCassandraCluster.FIRST_CASSANDRA_CONTAINER_NAME,
                nodetoolCommand);
    }

    private String runCommandInCliContainer(String... arguments) throws IOException,
            InterruptedException {
        return dockerComposeRule.run(
                DockerComposeRunOption.options("-T"),
                ThreeNodeCassandraCluster.CLI_CONTAINER_NAME,
                DockerComposeRunArgument.arguments(arguments));
    }
}
