/**
 * Copyright 2015 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.cassandra;

import static com.palantir.docker.compose.connection.waiting.HealthChecks.toHaveAllPortsOpen;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.Duration;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.palantir.docker.compose.DockerComposition;

@RunWith(Suite.class)
@SuiteClasses({
        CassandraKeyValueServiceSerializableTransactionTest.class,
        CassandraKeyValueServiceTransactionTest.class,
        CQLKeyValueServiceSerializableTransactionTest.class,
        CQLKeyValueServiceTransactionTest.class,
        CassandraTimestampTest.class,
        CassandraKeyValueServiceSweeperTest.class,
        CQLKeyValueServiceSweeperTest.class
})
public class CassandraTestSuite {

    private final static String DOCKER_HOST_VARIABLE = "DOCKER_HOST";
    static String CASSANDRA_HOST;

    @BeforeClass
    public static void initializeVariables() {
        String dockerHost = System.getenv(DOCKER_HOST_VARIABLE);
        if (dockerHost == null) {
            throw new IllegalStateException("Environment variable " + DOCKER_HOST_VARIABLE + " needs to be defined.");
        }
        CASSANDRA_HOST = extractCassandraHostName(dockerHost);
    }

    // DOCKER_HOST syntax: tcp://192.168.99.100:2376
    private static String extractCassandraHostName(String dockerHost) {
        Pattern pattern = Pattern.compile("://(.*?):");
        Matcher matcher = pattern.matcher(dockerHost);
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            return dockerHost;
        }
    }

    @ClassRule
    public static DockerComposition composition = DockerComposition.of("../docker-containers/docker-compose.yml")
            .saveLogsTo(".")
            .waitingForService("cassandra", toHaveAllPortsOpen(), Duration.standardMinutes(2))
            .build();
}
