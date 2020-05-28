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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.http.v2.ClientOptionsConstants;
import com.palantir.atlasdb.timelock.api.ConjureLockRequest;
import com.palantir.atlasdb.timelock.api.ConjureLockResponse;
import com.palantir.atlasdb.timelock.api.ConjureLockToken;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequest;
import com.palantir.atlasdb.timelock.api.SuccessfulLockResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulLockResponse;
import com.palantir.atlasdb.timelock.suite.SingleLeaderPaxosSuite;
import com.palantir.atlasdb.timelock.util.ExceptionMatchers;
import com.palantir.atlasdb.timelock.util.ParameterInjector;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.client.ConjureLockRequests;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.tokens.auth.AuthHeader;

@RunWith(Parameterized.class)
public class MultiNodePaxosTimeLockServerIntegrationTest {

    @ClassRule
    public static ParameterInjector<TestableTimelockCluster> injector =
            ParameterInjector.withFallBackConfiguration(() -> SingleLeaderPaxosSuite.BATCHED_TIMESTAMP_PAXOS);

    @Parameterized.Parameter
    public TestableTimelockCluster cluster;

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<TestableTimelockCluster> params() {
        return injector.getParameter();
    }

    private static final TestableTimelockCluster FIRST_CLUSTER = params().iterator().next();

    private static final LockDescriptor LOCK = StringLockDescriptor.of("foo");
    private static final Set<LockDescriptor> LOCKS = ImmutableSet.of(LOCK);

    private static final int DEFAULT_LOCK_TIMEOUT_MS = 10_000;
    private static final int LONGER_THAN_READ_TIMEOUT_LOCK_TIMEOUT_MS =
            Ints.saturatedCast(ClientOptionsConstants.SHORT_READ_TIMEOUT
                    .toJavaDuration()
                    .plus(Duration.ofSeconds(1)).toMillis());

    private NamespacedClients client;

    @Before
    public void bringAllNodesOnline() {
        client = cluster.clientForRandomNamespace().throughWireMockProxy();
        cluster.waitUntilAllServersOnlineAndReadyToServeNamespaces(ImmutableList.of(client.namespace()));
    }

    @Test
    public void nonLeadersReturn503() {
        cluster.nonLeaders(client.namespace()).forEach((namespace, server) -> {
            assertThatThrownBy(() -> server.client(namespace).getFreshTimestamp())
                    .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);
            assertThatThrownBy(() ->
                    server.client(namespace).lock(LockRequest.of(LOCKS, DEFAULT_LOCK_TIMEOUT_MS)))
                    .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);
        });
    }


    @Test
    public void nonLeadersReturn503_conjure() {
        cluster.nonLeaders(client.namespace()).forEach((namespace, server) -> {
            assertThatThrownBy(() -> server.client(namespace).namespacedConjureTimelockService().leaderTime())
                    .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);
        });
    }

    @Test
    public void leaderRespondsToRequests() {
        NamespacedClients currentLeader = cluster.currentLeaderFor(client.namespace())
                .client(client.namespace());
        currentLeader.getFreshTimestamp();

        LockToken token = currentLeader.lock(LockRequest.of(LOCKS, DEFAULT_LOCK_TIMEOUT_MS)).getToken();
        currentLeader.unlock(token);
    }

    @Test
    public void newLeaderTakesOverIfCurrentLeaderDies() {
        cluster.currentLeaderFor(client.namespace()).killSync();

        assertThatCode(client::getFreshTimestamp)
                .doesNotThrowAnyException();
    }

    @Test
    public void canUseNamespaceStartingWithTlOnLegacyEndpoints() {
        cluster.client("tl" + "suffix").throughWireMockProxy().getFreshTimestamp();
    }

    @Test
    public void leaderLosesLeadershipIfQuorumIsNotAlive() throws ExecutionException {
        NamespacedClients leader = cluster.currentLeaderFor(client.namespace())
                .client(client.namespace());
        cluster.killAndAwaitTermination(cluster.nonLeaders(client.namespace()).values());

        assertThatThrownBy(leader::getFreshTimestamp)
                .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);
    }

    @Test
    public void someoneBecomesLeaderAgainAfterQuorumIsRestored() throws ExecutionException {
        Set<TestableTimelockServer> nonLeaders = ImmutableSet.copyOf(cluster.nonLeaders(client.namespace()).values());
        cluster.killAndAwaitTermination(nonLeaders);

        nonLeaders.forEach(TestableTimelockServer::start);
        client.getFreshTimestamp();
    }

    @Test
    public void canHostilelyTakeOverNamespace() {
        TestableTimelockServer currentLeader = cluster.currentLeaderFor(client.namespace());
        TestableTimelockServer nonLeader = Iterables.get(
                cluster.nonLeaders(client.namespace()).get(client.namespace()), 0);

        assertThatThrownBy(nonLeader.client(client.namespace())::getFreshTimestamp)
                .as("non leader is not the leader before the takeover - sanity check")
                .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);

        assertThat(nonLeader.takeOverLeadershipForNamespace(client.namespace()))
                .as("successfully took over namespace")
                .isTrue();

        assertThatThrownBy(currentLeader.client(client.namespace())::getFreshTimestamp)
                .as("previous leader is no longer the leader after the takeover")
                .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);

        assertThat(cluster.currentLeaderFor(client.namespace()))
                .as("new leader is the previous non leader (hostile takeover)")
                .isEqualTo(nonLeader);
    }

    @Test
    public void canPerformRollingRestart() {
        bringAllNodesOnline();
        for (TestableTimelockServer server : cluster.servers()) {
            server.killSync();
            cluster.waitUntilAllServersOnlineAndReadyToServeNamespaces(ImmutableList.of(client.namespace()));
            client.getFreshTimestamp();
            server.start();
        }
    }

    @Test
    public void timestampsAreIncreasingAcrossFailovers() {
        long lastTimestamp = client.getFreshTimestamp();

        for (int i = 0; i < 3; i++) {
            cluster.failoverToNewLeader(client.namespace());

            long timestamp = client.getFreshTimestamp();
            assertThat(timestamp).isGreaterThan(lastTimestamp);
            lastTimestamp = timestamp;
        }
    }

    @Test
    public void leaderIdChangesAcrossFailovers() {
        Set<LeaderTime> leaderTimes = new HashSet<>();
        leaderTimes.add(client.namespacedConjureTimelockService().leaderTime());

        for (int i = 0; i < 3; i++) {
            cluster.failoverToNewLeader(client.namespace());

            LeaderTime leaderTime = client.namespacedConjureTimelockService().leaderTime();

            leaderTimes.forEach(previousLeaderTime ->
                    assertThat(previousLeaderTime.isComparableWith(leaderTime)).isFalse());
            leaderTimes.add(leaderTime);
        }
    }

    @Test
    public void locksAreInvalidatedAcrossFailovers() {
        LockToken token = client.lock(LockRequest.of(LOCKS, DEFAULT_LOCK_TIMEOUT_MS)).getToken();

        for (int i = 0; i < 3; i++) {
            cluster.failoverToNewLeader(client.namespace());

            assertThat(client.unlock(token)).isFalse();
            token = client.lock(LockRequest.of(LOCKS, DEFAULT_LOCK_TIMEOUT_MS)).getToken();
        }
    }

    @Test
    public void canCreateNewClientsDynamically() {
        for (int i = 0; i < 5; i++) {
            NamespacedClients randomNamespace = cluster.clientForRandomNamespace().throughWireMockProxy();

            randomNamespace.getFreshTimestamp();
            LockToken token = randomNamespace.lock(LockRequest.of(LOCKS, DEFAULT_LOCK_TIMEOUT_MS)).getToken();
            randomNamespace.unlock(token);
        }
    }

    @Test
    public void lockRequestCanBlockForTheFullTimeout() {
        abandonLeadershipPaxosModeAgnosticTestIfRunElsewhere();
        LockToken token = client.lock(LockRequest.of(LOCKS, DEFAULT_LOCK_TIMEOUT_MS)).getToken();

        try {
            LockResponse response = client.lock(LockRequest.of(LOCKS, LONGER_THAN_READ_TIMEOUT_LOCK_TIMEOUT_MS));
            assertThat(response.wasSuccessful()).isFalse();
        } finally {
            client.unlock(token);
        }
    }

    @Test
    public void waitForLocksRequestCanBlockForTheFullTimeout() {
        abandonLeadershipPaxosModeAgnosticTestIfRunElsewhere();
        LockToken token = client.lock(LockRequest.of(LOCKS, DEFAULT_LOCK_TIMEOUT_MS)).getToken();

        try {
            WaitForLocksResponse response = client.waitForLocks(WaitForLocksRequest.of(LOCKS,
                    LONGER_THAN_READ_TIMEOUT_LOCK_TIMEOUT_MS));
            assertThat(response.wasSuccessful()).isFalse();
        } finally {
            client.unlock(token);
        }
    }

    @Test
    public void multipleLockRequestsWithTheSameIdAreGranted() {
        ConjureLockRequest conjureLockRequest = ConjureLockRequests.toConjure(
                LockRequest.of(LOCKS, DEFAULT_LOCK_TIMEOUT_MS));

        Optional<ConjureLockToken> token1 = client.namespacedConjureTimelockService().lock(conjureLockRequest)
                .accept(ToConjureLockTokenVisitor.INSTANCE);
        Optional<ConjureLockToken> token2 = Optional.empty();
        try {
            token2 = client.namespacedConjureTimelockService().lock(conjureLockRequest)
                    .accept(ToConjureLockTokenVisitor.INSTANCE);

            assertThat(token1).isPresent();
            assertThat(token1).isEqualTo(token2);
        } finally {
            Set<ConjureLockToken> tokens = Stream.of(token1, token2)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toSet());
            client.namespacedConjureTimelockService().unlock(ConjureUnlockRequest.of(tokens));
        }
    }

    @Test
    public void canGetAllNamespaces() {
        String randomNamespace = UUID.randomUUID().toString();
        cluster.client(randomNamespace).throughWireMockProxy().getFreshTimestamp();

        Set<String> knownNamespaces = getKnownNamespaces();
        assertThat(knownNamespaces).contains(randomNamespace);

        for (TestableTimelockServer server : cluster.servers()) {
            server.killSync();
            server.start();
        }

        cluster.waitUntilAllServersOnlineAndReadyToServeNamespaces(
                ImmutableList.of(cluster.clientForRandomNamespace().namespace()));

        Set<String> namespacesAfterRestart = getKnownNamespaces();
        assertThat(namespacesAfterRestart).contains(randomNamespace);
        assertThat(Sets.difference(knownNamespaces, namespacesAfterRestart)).isEmpty();
    }

    private Set<String> getKnownNamespaces() {
        return cluster.servers()
                .stream()
                .map(TestableTimelockServer::timeLockManagementService)
                .map(resource -> resource.getNamespaces(AuthHeader.valueOf("Bearer omitted")))
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    @Test
    public void stressTest() {
        abandonLeadershipPaxosModeAgnosticTestIfRunElsewhere();
        TestableTimelockServer nonLeader = Iterables.getFirst(cluster.nonLeaders(client.namespace()).values(), null);
        int startingNumThreads = ManagementFactory.getThreadMXBean().getThreadCount();
        boolean isNonLeaderTakenOut = false;
        try {
            for (int i = 0; i < 10_000; i++) { // Needed as it takes a while for the thread buildup to occur
                client.getFreshTimestamp();
                assertNumberOfThreadsReasonable(
                        startingNumThreads,
                        ManagementFactory.getThreadMXBean().getThreadCount(),
                        isNonLeaderTakenOut);
                if (i == 1_000) {
                    makeServerWaitTwoSecondsAndThenReturn503s(nonLeader);
                    isNonLeaderTakenOut = true;
                }
            }
        } finally {
            nonLeader.serverHolder().resetWireMock();
        }
    }

    private static void assertNumberOfThreadsReasonable(int startingThreads, int threadCount, boolean nonLeaderDown) {
        // TODO (jkong): Lower the amount over the threshold. This needs to be slightly higher for now because of the
        // current threading model in batch mode, where separate threads may be spun up on the autobatcher.
        int threadLimit = startingThreads + 400;
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
    }

    private void makeServerWaitTwoSecondsAndThenReturn503s(TestableTimelockServer nonLeader) {
        nonLeader.serverHolder().wireMock().register(
                WireMock.any(WireMock.anyUrl())
                        .atPriority(Integer.MAX_VALUE - 1)
                        .willReturn(WireMock.serviceUnavailable().withFixedDelay(
                                Ints.checkedCast(Duration.ofSeconds(2).toMillis()))).build());
    }

    private enum ToConjureLockTokenVisitor implements ConjureLockResponse.Visitor<Optional<ConjureLockToken>> {
        INSTANCE;

        @Override
        public Optional<ConjureLockToken> visitSuccessful(SuccessfulLockResponse value) {
            return Optional.of(value.getLockToken());
        }

        @Override
        public Optional<ConjureLockToken> visitUnsuccessful(UnsuccessfulLockResponse value) {
            return Optional.empty();
        }

        @Override
        public Optional<ConjureLockToken> visitUnknown(String unknownType) {
            throw new RuntimeException("Unexpected type " + unknownType);
        }
    }
}
