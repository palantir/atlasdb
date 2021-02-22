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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.http.v2.ClientOptionsConstants;
import com.palantir.atlasdb.timelock.api.ConjureLockRequest;
import com.palantir.atlasdb.timelock.api.ConjureLockResponse;
import com.palantir.atlasdb.timelock.api.ConjureLockToken;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequest;
import com.palantir.atlasdb.timelock.api.LeaderTimes;
import com.palantir.atlasdb.timelock.api.MultiClientConjureTimelockService;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.SuccessfulLockResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulLockResponse;
import com.palantir.atlasdb.timelock.suite.DbTimeLockSingleLeaderPaxosSuite;
import com.palantir.atlasdb.timelock.util.ExceptionMatchers;
import com.palantir.atlasdb.timelock.util.ParameterInjector;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.ConjureLockRefreshToken;
import com.palantir.lock.ConjureLockV1Request;
import com.palantir.lock.ConjureSimpleHeldLocksToken;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.SimpleHeldLocksToken;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.client.ConjureLockRequests;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LeadershipId;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.PartitionedTimestamps;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.tokens.auth.AuthHeader;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

@RunWith(Parameterized.class)
public class MultiNodePaxosTimeLockServerIntegrationTest {

    @ClassRule
    public static ParameterInjector<TestableTimelockCluster> injector =
            ParameterInjector.withFallBackConfiguration(() -> DbTimeLockSingleLeaderPaxosSuite.DB_TIMELOCK_CLUSTER);

    @Parameterized.Parameter
    public TestableTimelockCluster cluster;

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<TestableTimelockCluster> params() {
        return injector.getParameter();
    }

    private static final TestableTimelockCluster FIRST_CLUSTER =
            params().iterator().next();

    private static final LockDescriptor LOCK = StringLockDescriptor.of("foo");
    private static final ImmutableSet<LockDescriptor> LOCKS = ImmutableSet.of(LOCK);

    private static final int DEFAULT_LOCK_TIMEOUT_MS = 10_000;
    private static final int LONGER_THAN_READ_TIMEOUT_LOCK_TIMEOUT_MS =
            Ints.saturatedCast(ClientOptionsConstants.SHORT_READ_TIMEOUT
                    .toJavaDuration()
                    .plus(Duration.ofSeconds(1))
                    .toMillis());
    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("Bearer omitted");

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
            assertThatThrownBy(() -> server.client(namespace).lock(LockRequest.of(LOCKS, DEFAULT_LOCK_TIMEOUT_MS)))
                    .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);
        });
    }

    @Test
    public void nonLeadersReturn503_conjure() {
        cluster.nonLeaders(client.namespace()).forEach((namespace, server) -> {
            assertThatThrownBy(() -> server.client(namespace)
                            .namespacedConjureTimelockService()
                            .leaderTime())
                    .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);
        });
    }

    @Test
    public void leaderRespondsToRequests() {
        NamespacedClients currentLeader =
                cluster.currentLeaderFor(client.namespace()).client(client.namespace());
        currentLeader.getFreshTimestamp();

        LockToken token = currentLeader
                .lock(LockRequest.of(LOCKS, DEFAULT_LOCK_TIMEOUT_MS))
                .getToken();
        currentLeader.unlock(token);
    }

    @Test
    public void newLeaderTakesOverIfCurrentLeaderDies() {
        cluster.currentLeaderFor(client.namespace()).killSync();

        assertThatCode(client::getFreshTimestamp).doesNotThrowAnyException();
    }

    @Test
    public void canUseNamespaceStartingWithTlOnLegacyEndpoints() {
        cluster.client("tl" + "suffix").throughWireMockProxy().getFreshTimestamp();
    }

    @Test
    public void leaderLosesLeadershipIfQuorumIsNotAlive() throws ExecutionException {
        NamespacedClients leader = cluster.currentLeaderFor(client.namespace()).client(client.namespace());
        cluster.killAndAwaitTermination(cluster.nonLeaders(client.namespace()).values());

        assertThatThrownBy(leader::getFreshTimestamp)
                .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);
    }

    @Test
    public void someoneBecomesLeaderAgainAfterQuorumIsRestored() throws ExecutionException {
        Set<TestableTimelockServer> nonLeaders =
                ImmutableSet.copyOf(cluster.nonLeaders(client.namespace()).values());
        cluster.killAndAwaitTermination(nonLeaders);

        nonLeaders.forEach(TestableTimelockServer::start);
        client.getFreshTimestamp();
    }

    @Test
    public void canHostilelyTakeOverNamespace() {
        TestableTimelockServer currentLeader = cluster.currentLeaderFor(client.namespace());
        TestableTimelockServer nonLeader =
                Iterables.get(cluster.nonLeaders(client.namespace()).get(client.namespace()), 0);

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
        LockToken token =
                client.lock(LockRequest.of(LOCKS, DEFAULT_LOCK_TIMEOUT_MS)).getToken();

        for (int i = 0; i < 3; i++) {
            cluster.failoverToNewLeader(client.namespace());

            assertThat(client.unlock(token)).isFalse();
            token = client.lock(LockRequest.of(LOCKS, DEFAULT_LOCK_TIMEOUT_MS)).getToken();
        }
    }

    @Test
    public void canCreateNewClientsDynamically() {
        for (int i = 0; i < 5; i++) {
            NamespacedClients randomNamespace =
                    cluster.clientForRandomNamespace().throughWireMockProxy();

            randomNamespace.getFreshTimestamp();
            LockToken token = randomNamespace
                    .lock(LockRequest.of(LOCKS, DEFAULT_LOCK_TIMEOUT_MS))
                    .getToken();
            randomNamespace.unlock(token);
        }
    }

    @Test
    public void lockRequestCanBlockForTheFullTimeout() {
        abandonLeadershipPaxosModeAgnosticTestIfRunElsewhere();
        LockToken token =
                client.lock(LockRequest.of(LOCKS, DEFAULT_LOCK_TIMEOUT_MS)).getToken();

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
        LockToken token =
                client.lock(LockRequest.of(LOCKS, DEFAULT_LOCK_TIMEOUT_MS)).getToken();

        try {
            WaitForLocksResponse response =
                    client.waitForLocks(WaitForLocksRequest.of(LOCKS, LONGER_THAN_READ_TIMEOUT_LOCK_TIMEOUT_MS));
            assertThat(response.wasSuccessful()).isFalse();
        } finally {
            client.unlock(token);
        }
    }

    @Test
    public void multipleLockRequestsWithTheSameIdAreGranted() {
        ConjureLockRequest conjureLockRequest =
                ConjureLockRequests.toConjure(LockRequest.of(LOCKS, DEFAULT_LOCK_TIMEOUT_MS));

        Optional<ConjureLockToken> token1 = client.namespacedConjureTimelockService()
                .lock(conjureLockRequest)
                .accept(ToConjureLockTokenVisitor.INSTANCE);
        Optional<ConjureLockToken> token2 = Optional.empty();
        try {
            token2 = client.namespacedConjureTimelockService()
                    .lock(conjureLockRequest)
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
        assertThat(namespacesAfterRestart).doesNotContain("learner", "acceptor");
        assertThat(Sets.difference(knownNamespaces, namespacesAfterRestart)).isEmpty();
    }

    private Set<String> getKnownNamespaces() {
        return cluster.servers().stream()
                .map(TestableTimelockServer::timeLockManagementService)
                .map(resource -> resource.getNamespaces(AuthHeader.valueOf("Bearer omitted")))
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    @Test
    public void directLegacyAndConjureLockServicesInteractCorrectly() throws InterruptedException {
        LockRefreshToken token = client.legacyLockService()
                .lock(
                        "tom",
                        com.palantir.lock.LockRequest.builder(
                                        ImmutableSortedMap.<LockDescriptor, LockMode>naturalOrder()
                                                .put(StringLockDescriptor.of("lock"), LockMode.WRITE)
                                                .build())
                                .build());
        ConjureLockRefreshToken conjureAnalogue =
                ConjureLockRefreshToken.of(token.getTokenId(), token.getExpirationDateMs());

        // Cannot assert equality because tokens can have different expiration dates.
        assertThat(client.legacyLockService().refreshLockRefreshTokens(ImmutableList.of(token)))
                .as("refreshing a live token should succeed")
                .hasOnlyOneElementSatisfying(
                        refreshed -> assertThat(refreshed.getTokenId()).isEqualTo(refreshed.getTokenId()));
        AuthHeader authHeader = AuthHeader.valueOf("Bearer unused");
        assertThat(client.conjureLegacyLockService()
                        .refreshLockRefreshTokens(authHeader, client.namespace(), ImmutableList.of(conjureAnalogue)))
                .as("it is possible to refresh a live token through the conjure API")
                .hasOnlyOneElementSatisfying(
                        refreshed -> assertThat(refreshed.getTokenId()).isEqualTo(refreshed.getTokenId()));

        ConjureSimpleHeldLocksToken conjureHeldLocksToken = ConjureSimpleHeldLocksToken.of(token.getTokenId(), 0L);
        assertThat(client.conjureLegacyLockService()
                        .unlockSimple(authHeader, client.namespace(), conjureHeldLocksToken))
                .as("it is possible to unlock a live token through the conjure API")
                .isTrue();
        assertThat(client.conjureLegacyLockService()
                        .unlockSimple(authHeader, client.namespace(), conjureHeldLocksToken))
                .as("a token unlocked through the conjure API stays unlocked")
                .isFalse();
        assertThat(client.legacyLockService().unlockSimple(SimpleHeldLocksToken.fromLockRefreshToken(token)))
                .as("a token unlocked through the conjure API stays unlocked even in the legacy API")
                .isFalse();
    }

    @Test
    public void lockAcquiredByConjureLockServiceIsAlsoAcquiredInLegacy() throws InterruptedException {
        com.palantir.lock.LockRequest lockRequest = com.palantir.lock.LockRequest.builder(
                        ImmutableSortedMap.<LockDescriptor, LockMode>naturalOrder()
                                .put(StringLockDescriptor.of("lock"), LockMode.WRITE)
                                .build())
                .doNotBlock()
                .build();
        String anonymousId = LockClient.ANONYMOUS.getClientId();
        ConjureLockV1Request conjureLockRequest = ConjureLockV1Request.builder()
                .lockClient(anonymousId)
                .lockRequest(lockRequest)
                .build();

        HeldLocksToken token = client.conjureLegacyLockService()
                .lockAndGetHeldLocks(AuthHeader.valueOf("Bearer unused"), client.namespace(), conjureLockRequest)
                .orElseThrow(() -> new RuntimeException("We should have been able to get the lock"));

        assertThat(client.legacyLockService().lockAndGetHeldLocks(anonymousId, lockRequest))
                .as("if the conjure impl has taken a lock, the legacy impl mustn't be able to take it")
                .isNull();
        assertThat(client.legacyLockService().unlock(token.getLockRefreshToken()))
                .as("legacy impl can unlock a lock taken by conjure impl")
                .isTrue();
        assertThat(client.legacyLockService().lockAndGetHeldLocks(anonymousId, lockRequest))
                .as("lock taken by conjure impl that was unlocked can now be acquired by legacy impl")
                .isNotNull();
        assertThat(client.conjureLegacyLockService()
                        .lockAndGetHeldLocks(
                                AuthHeader.valueOf("Bearer unused"), client.namespace(), conjureLockRequest))
                .as("if the legacy impl has taken a lock, the conjure impl mustn't be able to take it")
                .isEmpty();
    }

    @Test
    public void fastForwardDoesNotGoBack() {
        long freshTimestamp = client.getFreshTimestamp();

        long fastForwardTimestamp = freshTimestamp + 100_000_000;
        client.timestampManagementService().fastForwardTimestamp(fastForwardTimestamp);
        client.timestampManagementService().fastForwardTimestamp(freshTimestamp);
        assertThat(client.getFreshTimestamp()).isGreaterThan(fastForwardTimestamp);
    }

    @Test
    public void fastForwardsDoNotHaveCrossNamespaceImpact() {
        long freshTimestamp = client.getFreshTimestamp();

        long fastForwardTimestamp = freshTimestamp + 100_000_000;
        NamespacedClients other = cluster.clientForRandomNamespace().throughWireMockProxy();
        other.timestampManagementService().fastForwardTimestamp(fastForwardTimestamp);

        cluster.failoverToNewLeader(client.namespace());
        assertThat(client.getFreshTimestamp()).isGreaterThan(freshTimestamp).isLessThan(fastForwardTimestamp);
        assertThat(other.getFreshTimestamp()).isGreaterThan(fastForwardTimestamp);
    }

    @Test
    public void sanityCheckMultiClientLeaderTime() {
        Set<Namespace> expectedNamespaces = ImmutableSet.of(Namespace.of("client1"), Namespace.of("client2"));
        LeaderTimes leaderTimes = assertSanityAndGetLeaderTimes(expectedNamespaces);

        // leaderTimes for namespaces are computed by their respective underlying AsyncTimelockService instances
        Set<UUID> leadershipIds = leaderTimes.getLeaderTimes().values().stream()
                .map(LeaderTime::id)
                .map(LeadershipId::id)
                .collect(Collectors.toSet());
        assertThat(leadershipIds).hasSameSizeAs(expectedNamespaces);
    }

    @Test
    public void sanityCheckMultiClientLeaderTimeAgainstConjureTimelockService() {
        Set<Namespace> expectedNamespaces = ImmutableSet.of(Namespace.of("alpha"), Namespace.of("beta"));
        TestableTimelockServer leader = cluster.currentLeaderFor(client.namespace());
        LeaderTimes leaderTimes = assertSanityAndGetLeaderTimes(expectedNamespaces);

        // Whether we hit the multi client endpoint or conjureTimelockService endpoint(services one client in one
        // call), for a namespace, the underlying service to process the request is the same
        leaderTimes.getLeaderTimes().forEach((namespace, leaderTime) -> {
            LeaderTime conjureTimelockServiceLeaderTime = leader.client(namespace.get())
                    .namespacedConjureTimelockService()
                    .leaderTime();
            assertThat(conjureTimelockServiceLeaderTime.id()).isEqualTo(leaderTime.id());
        });
    }

    @Test
    public void sanityCheckMultiClientStartTransactions() {
        TestableTimelockServer leader = cluster.currentLeaderFor(client.namespace());
        List<String> expectedNamespaces = ImmutableList.of("alpha", "beta");

        Map<Namespace, ConjureStartTransactionsResponse> startTransactions =
                assertSanityAndStartTransactions(leader, expectedNamespaces);

        Set<UUID> leadershipIds = startTransactions.values().stream()
                .map(ConjureStartTransactionsResponse::getLockWatchUpdate)
                .map(LockWatchStateUpdate::logId)
                .collect(Collectors.toSet());
        assertThat(leadershipIds).hasSameSizeAs(expectedNamespaces);
    }

    @Test
    public void sanityCheckMultiClientStartTransactionsAgainstConjureTimelockService() {
        TestableTimelockServer leader = cluster.currentLeaderFor(client.namespace());
        MultiClientConjureTimelockService multiClientConjureTimelockService = leader.multiClientService();

        List<String> expectedNamespaces = ImmutableList.of("alpha", "beta");
        int numTransactions = 7;
        Map<Namespace, ConjureStartTransactionsRequest> namespaceToRequestMap =
                defaultNamespacedStartTransactionsRequests(expectedNamespaces, numTransactions);

        Map<Namespace, ConjureStartTransactionsResponse> startedTransactions =
                multiClientConjureTimelockService.startTransactions(AUTH_HEADER, namespaceToRequestMap);

        // Whether we hit the multi client endpoint or conjureTimelockService endpoint, for a namespace, the underlying
        // service to process the request is the same
        startedTransactions.forEach((namespace, responseFromBatchedEndpoint) -> {
            ConjureStartTransactionsResponse responseFromLegacyEndpoint = leader.client(namespace.get())
                    .namespacedConjureTimelockService()
                    .startTransactions(namespaceToRequestMap.get(namespace));
            assertThat(responseFromLegacyEndpoint.getLockWatchUpdate().logId())
                    .isEqualTo(responseFromBatchedEndpoint.getLockWatchUpdate().logId());

            PartitionedTimestamps batchedEndpointTimestamps = responseFromBatchedEndpoint.getTimestamps();
            assertThat(responseFromLegacyEndpoint.getTimestamps().start())
                    .isEqualTo(
                            batchedEndpointTimestamps.start() + numTransactions * batchedEndpointTimestamps.interval());
        });
    }

    private Map<Namespace, ConjureStartTransactionsResponse> assertSanityAndStartTransactions(
            TestableTimelockServer leader, List<String> expectedNamespaces) {
        MultiClientConjureTimelockService multiClientConjureTimelockService = leader.multiClientService();
        int numTransactions = 5;

        Map<Namespace, ConjureStartTransactionsRequest> namespaceToRequestMap =
                defaultNamespacedStartTransactionsRequests(expectedNamespaces, numTransactions);

        Map<Namespace, ConjureStartTransactionsResponse> startedTransactions =
                multiClientConjureTimelockService.startTransactions(AUTH_HEADER, namespaceToRequestMap);

        Set<String> namespaces =
                startedTransactions.keySet().stream().map(Namespace::get).collect(Collectors.toSet());
        assertThat(namespaces).hasSameElementsAs(expectedNamespaces);

        assertThat(startedTransactions.values().stream()
                        .map(ConjureStartTransactionsResponse::getTimestamps)
                        .mapToLong(partitionedTimestamps ->
                                partitionedTimestamps.stream().count())
                        .sum())
                .isEqualTo(namespaces.size() * numTransactions);
        return startedTransactions;
    }

    private Map<Namespace, ConjureStartTransactionsRequest> defaultNamespacedStartTransactionsRequests(
            List<String> namespaces, int numTransactions) {
        return KeyedStream.of(namespaces)
                .map(namespace -> ConjureStartTransactionsRequest.builder()
                        .numTransactions(numTransactions)
                        .requestId(UUID.randomUUID())
                        .requestorId(UUID.randomUUID())
                        .build())
                .mapKeys(Namespace::of)
                .collectToMap();
    }

    private LeaderTimes assertSanityAndGetLeaderTimes(Set<Namespace> expectedNamespaces) {
        TestableTimelockServer leader = cluster.currentLeaderFor(client.namespace());
        MultiClientConjureTimelockService multiClientConjureTimelockService = leader.multiClientService();

        LeaderTimes leaderTimes = multiClientConjureTimelockService.leaderTimes(AUTH_HEADER, expectedNamespaces);
        Set<Namespace> namespaces = leaderTimes.getLeaderTimes().keySet();
        assertThat(namespaces).hasSameElementsAs(expectedNamespaces);

        return leaderTimes;
    }

    private void abandonLeadershipPaxosModeAgnosticTestIfRunElsewhere() {
        Assume.assumeTrue(cluster == FIRST_CLUSTER);
        Assume.assumeFalse(cluster.isDbTimelock()); // We will never test only DB timelock when releasing.
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
