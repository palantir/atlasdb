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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;
import com.palantir.docker.compose.execution.DockerComposeRunArgument;
import com.palantir.docker.compose.execution.DockerComposeRunOption;

public class ThreeNodeCassandraCluster extends Container {
    private static final Logger log = LoggerFactory.getLogger(ThreeNodeCassandraCluster.class);

    public static final String CLI_CONTAINER_NAME = "cli";
    public static final String FIRST_CASSANDRA_CONTAINER_NAME = "cassandra1";
    public static final String SECOND_CASSANDRA_CONTAINER_NAME = "cassandra2";
    public static final String THIRD_CASSANDRA_CONTAINER_NAME = "cassandra3";
    public static final int CASSANDRA_PORT = 9160;
    public static final String USERNAME = "cassandra";
    public static final String PASSWORD = "cassandra";

    public static final CassandraKeyValueServiceConfig KVS_CONFIG = ImmutableCassandraKeyValueServiceConfig.builder()
            .addServers(new InetSocketAddress(FIRST_CASSANDRA_CONTAINER_NAME, CASSANDRA_PORT))
            .addServers(new InetSocketAddress(SECOND_CASSANDRA_CONTAINER_NAME, CASSANDRA_PORT))
            .addServers(new InetSocketAddress(THIRD_CASSANDRA_CONTAINER_NAME, CASSANDRA_PORT))
            .poolSize(20)
            .keyspace("atlasdb")
            .credentials(ImmutableCassandraCredentialsConfig.builder()
                    .username(USERNAME)
                    .password(PASSWORD)
                    .build())
            .replicationFactor(3)
            .mutationBatchCount(10000)
            .mutationBatchSizeBytes(10000000)
            .fetchBatchCount(1000)
            .safetyDisabled(false)
            .autoRefreshNodes(false)
            .build();

    public static final Optional<LeaderConfig> LEADER_CONFIG = Optional.of(ImmutableLeaderConfig
            .builder()
            .quorumSize(1)
            .localServer("localhost")
            .leaders(ImmutableSet.of("localhost"))
            .build());

    @Override
    public String getDockerComposeFile() {
        return "/docker-compose-cassandra-three-node.yml";
    }

    @Override
    public SuccessOrFailure isReady(DockerComposeRule rule) {
        return SuccessOrFailure.onResultOf(() -> {

            try {
                if (!nodetoolShowsThreeCassandraNodesUp(rule)) {
                    return false;
                }

                // slightly hijacking the isReady function here - using it
                // to actually modify the cluster
                replicateSystemAuthenticationDataOnAllNodes(rule);

                return canCreateCassandraKeyValueService();
            } catch (Exception e) {
                log.info("Exception while checking if the Cassandra cluster was ready: " + e);
                return false;
            }
        });
    }

    private boolean canCreateCassandraKeyValueService() {
        CassandraKeyValueService.create(
                CassandraKeyValueServiceConfigManager.createSimpleManager(KVS_CONFIG),
                LEADER_CONFIG);
        return true;
    }

    private void replicateSystemAuthenticationDataOnAllNodes(DockerComposeRule rule)
            throws IOException, InterruptedException {
        if (!systemAuthenticationKeyspaceHasReplicationFactorThree(rule)) {
            setReplicationFactorOfSystemAuthenticationKeyspaceToThree(rule);
            runNodetoolRepair(rule);
        }
    }

    private void runNodetoolRepair(DockerComposeRule rule) throws IOException, InterruptedException {
        runNodetoolCommand(rule, "repair system_auth");
    }

    private void setReplicationFactorOfSystemAuthenticationKeyspaceToThree(DockerComposeRule rule)
            throws IOException, InterruptedException {
        runCql(rule,
            "ALTER KEYSPACE system_auth " +
            "WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 3};");
    }

    private boolean systemAuthenticationKeyspaceHasReplicationFactorThree(DockerComposeRule rule)
            throws IOException, InterruptedException {
        String output = runCql(rule, "SELECT * FROM system.schema_keyspaces;");
        int replicationFactor = parseSystemAuthReplicationFromCqlsh(output);
        return replicationFactor == 3;
    }

    private boolean nodetoolShowsThreeCassandraNodesUp(DockerComposeRule rule) {
        try {
            String output = runNodetoolCommand(rule, "status");
            int numberNodesUp = parseNumberOfUpNodesFromNodetoolStatus(output);
            return numberNodesUp == 3;
        } catch (Exception e) {
            log.warn("Failed while running nodetool status: " + e);
            return false;
        }
    }

    public static int parseSystemAuthReplicationFromCqlsh(String output) throws IllegalArgumentException {
        try {
            for (String line : output.split("\n")) {
                if (line.contains("system_auth")) {
                    Matcher m = Pattern.compile("^.*\\{\"replication_factor\":\"(\\d+)\"\\}$").matcher(line);
                    m.find();
                    return Integer.valueOf(m.group(1));
                }
            }
        } catch (Exception e) {
            log.error("Failed parsing system_auth keyspace RF: " + e);
            throw new IllegalArgumentException("Cannot determine replication factor of system_auth keyspace");
        }

        throw new IllegalArgumentException("Cannot determine replication factor of system_auth keyspace");
    }

    public static int parseNumberOfUpNodesFromNodetoolStatus(String output) {
        Pattern r = Pattern.compile("^UN.*");
        int upNodes = 0;
        for (String line : output.split("\n")) {
            Matcher m = r.matcher(line);
            if (m.matches()) {
                upNodes++;
            }
        }
        return upNodes;
    }

    private String runCql(DockerComposeRule rule, String cql) throws IOException, InterruptedException {
        return runCommandInCliContainer(rule,
                "cqlsh",
                "--username", USERNAME,
                "--password", PASSWORD,
                FIRST_CASSANDRA_CONTAINER_NAME,
                "--execute", cql);
    }

    private String runNodetoolCommand(DockerComposeRule rule, String nodetoolCommand) throws IOException, InterruptedException {
        return runCommandInCliContainer(rule,
                "nodetool",
                "--host", FIRST_CASSANDRA_CONTAINER_NAME,
                nodetoolCommand);

    }

    private String runCommandInCliContainer(DockerComposeRule rule, String... arguments) throws IOException, InterruptedException {
        return rule.run(
                DockerComposeRunOption.options("-T"),
                CLI_CONTAINER_NAME,
                DockerComposeRunArgument.arguments(arguments));
    }

}
