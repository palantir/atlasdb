/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.timelock.stress.DbTimeLockSingleLeaderPaxosStressTests;
import com.palantir.atlasdb.timelock.util.ParameterInjector;
import com.palantir.tokens.auth.AuthHeader;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class MultiNodePaxosTimeLockServerStressTest {

    @ClassRule
    public static ParameterInjector<TestableTimelockCluster> injector =
            ParameterInjector.withFallBackConfiguration(() -> DbTimeLockSingleLeaderPaxosStressTests.DB_TIMELOCK_CLUSTER);

    @Parameterized.Parameter
    public TestableTimelockCluster cluster;

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<TestableTimelockCluster> params() {
        return injector.getParameter();
    }

    private static final TestableTimelockCluster FIRST_CLUSTER =
            params().iterator().next();

    private NamespacedClients client;

    @Before
    public void bringAllNodesOnline() {
        client = cluster.clientForRandomNamespace().throughWireMockProxy();
        cluster.waitUntilAllServersOnlineAndReadyToServeNamespaces(ImmutableList.of(client.namespace()));
    }

    @Test
    public void stressTest() {
        abandonLeadershipPaxosModeAgnosticTestIfRunElsewhere();
        TestableTimelockServer nonLeader =
                Iterables.getFirst(cluster.nonLeaders(client.namespace()).values(), null);
        int startingNumThreads = ManagementFactory.getThreadMXBean().getThreadCount();
        boolean isNonLeaderTakenOut = false;
        try {
            for (int i = 0; i < 10_000; i++) { // Needed as it takes a while for the thread buildup to occur
                client.getFreshTimestamp();
                assertNumberOfThreadsReasonable(
                        startingNumThreads, ManagementFactory.getThreadMXBean().getThreadCount(), isNonLeaderTakenOut);
                if (i == 1_000) {
                    makeServerWaitTwoSecondsAndThenReturn503s(nonLeader);
                    isNonLeaderTakenOut = true;
                }
            }
        } finally {
            nonLeader.serverHolder().resetWireMock();
        }
    }

    @Test
    public void stressTestForPaxosEndpoints() {
        abandonLeadershipPaxosModeAgnosticTestIfRunElsewhere();
        TestableTimelockServer nonLeader =
                Iterables.getFirst(cluster.nonLeaders(client.namespace()).values(), null);
        int startingNumThreads = ManagementFactory.getThreadMXBean().getThreadCount();
        boolean isNonLeaderTakenOut = false;
        try {
            for (int i = 0; i < 1_800; i++) { // Needed as it takes a while for the thread buildup to occur
                assertNumberOfThreadsReasonable(
                        startingNumThreads, ManagementFactory.getThreadMXBean().getThreadCount(), isNonLeaderTakenOut);
                cluster.currentLeaderFor(client.namespace())
                        .timeLockManagementService()
                        .achieveConsensus(AuthHeader.valueOf("Bearer pqrstuv"), ImmutableSet.of(client.namespace()));
                if (i == 400) {
                    makeServerWaitTwoSecondsAndThenReturn503s(nonLeader);
                    isNonLeaderTakenOut = true;
                }
            }
        } finally {
            nonLeader.serverHolder().resetWireMock();
        }
    }

    private static void assertNumberOfThreadsReasonable(int startingThreads, int threadCount, boolean nonLeaderDown) {
        int threadLimit = startingThreads + 1000;
        if (nonLeaderDown) {
            assertThat(threadCount)
                    .as("should not additionally spin up too many threads after a non-leader failed")
                    .isLessThanOrEqualTo(threadLimit);
        } else {
            assertThat(threadCount)
                    .as("should not additionally spin up too many threads in the absence of failures")
                    .isLessThanOrEqualTo(threadLimit);
        }
    }

    private void abandonLeadershipPaxosModeAgnosticTestIfRunElsewhere() {
        Assume.assumeTrue(cluster == FIRST_CLUSTER);
        Assume.assumeFalse(cluster.isDbTimelock()); // We will never test only DB timelock when releasing.
    }

    private void makeServerWaitTwoSecondsAndThenReturn503s(TestableTimelockServer nonLeader) {
        nonLeader
                .serverHolder()
                .wireMock()
                .register(WireMock.any(WireMock.anyUrl())
                        .atPriority(Integer.MAX_VALUE - 1)
                        .willReturn(WireMock.serviceUnavailable()
                                .withFixedDelay(
                                        Ints.checkedCast(Duration.ofSeconds(2).toMillis())))
                        .build());
    }
}
