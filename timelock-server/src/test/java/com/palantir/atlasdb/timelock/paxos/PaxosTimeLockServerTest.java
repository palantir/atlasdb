/*
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
package com.palantir.atlasdb.timelock.paxos;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.isA;
import static org.mockito.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.health.HealthCheckRegistry;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.http.NotCurrentLeaderExceptionMapper;
import com.palantir.atlasdb.timelock.TimeLockServer;
import com.palantir.atlasdb.timelock.config.ImmutableClusterConfiguration;
import com.palantir.atlasdb.timelock.config.ImmutablePaxosConfiguration;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;
import com.palantir.paxos.PaxosAcceptor;

import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.dropwizard.setup.Environment;

public class PaxosTimeLockServerTest {
    private static final File TEST_DATA_DIRECTORY = new File("testLogs/");
    private static final String LOCAL_ADDRESS_STRING = "localhost:8080";
    private static final Set<String> TEST_CLIENTS = ImmutableSet.of("test");

    private static final TimeLockServerConfiguration TIMELOCK_CONFIG = new TimeLockServerConfiguration(
            ImmutablePaxosConfiguration.builder()
                    .paxosDataDir(TEST_DATA_DIRECTORY)
                    .build(),
            ImmutableClusterConfiguration.builder()
                    .addServers(LOCAL_ADDRESS_STRING)
                    .localServer(LOCAL_ADDRESS_STRING)
                    .build(),
            TEST_CLIENTS);

    private final Environment environment = mock(Environment.class);
    private final TimeLockServer implementation =
            TIMELOCK_CONFIG.algorithm().createServerImpl(environment);

    @Before
    public void setUp() {
        when(environment.jersey()).thenReturn(mock(JerseyEnvironment.class));
        when(environment.healthChecks()).thenReturn(mock(HealthCheckRegistry.class));
    }

    @Test
    public void verifyPaxosResourcesAreRegisteredAfterStartup() throws IOException {
        implementation.onStartup(TIMELOCK_CONFIG);
        verify(environment.jersey(), times(1)).register(isA(LeadershipResource.class));
        verify(environment.jersey(), times(1)).register(isA(PaxosResource.class));
        verify(environment.jersey(), times(1)).register(isA(NotCurrentLeaderExceptionMapper.class));
    }

    @Test
    public void verifyHealthCheckIsRegisteredAfterStartup() throws IOException {
        implementation.onStartup(TIMELOCK_CONFIG);
        verify(environment.healthChecks(), times(1))
                .register(startsWith("leader-ping"), isA(LeaderPingHealthCheck.class));
    }

    @Test
    public void verifyQuorumOfOneNodeIsOne() {
        verifyQuorumSize(1, 1);
    }

    @Test
    public void verifyQuorumsOfMultipleNodesAreMajorities() {
        verifyQuorumSize(2, 2);
        verifyQuorumSize(3, 2);
        verifyQuorumSize(5, 3);
        verifyQuorumSize(7, 4);
        verifyQuorumSize(12, 7);
    }

    @Test
    public void verifyLargeQuorumsAreCorrect() {
        int halfOfLargeCluster = 84;
        verifyQuorumSize(halfOfLargeCluster * 2, halfOfLargeCluster + 1);
        verifyQuorumSize(halfOfLargeCluster * 2 + 1, halfOfLargeCluster + 1);
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(TEST_DATA_DIRECTORY);
    }

    private static void verifyQuorumSize(int nodes, int expected) {
        List<PaxosAcceptor> acceptorList = Lists.newArrayList();
        for (int i = 0; i < nodes; i++) {
            acceptorList.add(null);
        }
        assertEquals(expected, PaxosTimeLockServer.getQuorumSize(acceptorList));
    }
}
