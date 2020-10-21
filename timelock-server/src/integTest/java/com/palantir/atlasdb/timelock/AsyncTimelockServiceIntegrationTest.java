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
package com.palantir.atlasdb.timelock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codahale.metrics.Metric;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.timelock.api.ConjureLockDescriptor;
import com.palantir.atlasdb.timelock.api.ConjureLockRequest;
import com.palantir.atlasdb.timelock.api.ConjureLockResponse;
import com.palantir.atlasdb.timelock.api.ConjureLockToken;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequest;
import com.palantir.atlasdb.timelock.api.SuccessfulLockResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulLockResponse;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.conjure.java.lib.Bytes;
import com.palantir.lock.HeldLocksGrant;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.SimpleHeldLocksToken;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.SortedLockCollection;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.timestamp.TimestampRange;
import com.palantir.tritium.metrics.registry.MetricName;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.assertj.core.api.Condition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AsyncTimelockServiceIntegrationTest extends AbstractAsyncTimelockServiceIntegrationTest {

    private static final LockDescriptor LOCK_A = StringLockDescriptor.of("a");
    private static final LockDescriptor LOCK_B = StringLockDescriptor.of("b");
    private static final ConjureLockDescriptor CONJURE_LOCK_A = ConjureLockDescriptor.of(Bytes.from(LOCK_A.getBytes()));

    private static final int SHORT_TIMEOUT = 500;
    private static final int TIMEOUT = 10_000;
    private static final LockClient TEST_CLIENT = LockClient.of("test");
    private static final LockClient TEST_CLIENT_2 = LockClient.of("test2");
    private static final LockClient TEST_CLIENT_3 = LockClient.of("test3");

    private final ExecutorService executor = PTExecutors.newCachedThreadPool();

    private NamespacedClients namespace;

    @Before
    public void setUp() {
        namespace = cluster.clientForRandomNamespace();
    }

    @After
    public void after() {
        assertThat(namespace.legacyLockService().getTokens(TEST_CLIENT)).isEmpty();
        executor.shutdown();
    }

    @Test
    public void canLockRefreshAndUnlock() {
        LockToken token = namespace.lock(requestFor(LOCK_A)).getToken();
        boolean wasRefreshed = namespace.refreshLockLease(token);
        boolean wasUnlocked = namespace.unlock(token);

        assertThat(wasRefreshed).isTrue();
        assertThat(wasUnlocked).isTrue();
    }

    @Test
    public void locksAreExclusive() {
        LockToken token = namespace.lock(requestFor(LOCK_A)).getToken();
        Future<LockToken> futureToken = lockAsync(requestFor(LOCK_A)).thenApply(LockResponse::getToken);

        assertNotYetLocked(futureToken);

        namespace.unlock(token);

        assertLockedAndUnlock(futureToken);
    }

    private CompletableFuture<LockResponse> lockAsync(LockRequest requestV2) {
        return CompletableFuture.supplyAsync(() -> namespace.lock(requestV2), executor);
    }

    @Test
    public void canLockImmutableTimestamp() {
        LockImmutableTimestampResponse response1 = namespace.timelockService().lockImmutableTimestamp();
        LockImmutableTimestampResponse response2 = namespace.timelockService().lockImmutableTimestamp();

        long immutableTs = namespace.timelockService().getImmutableTimestamp();
        assertThat(immutableTs).isEqualTo(response1.getImmutableTimestamp());

        namespace.unlock(response1.getLock());

        assertThat(immutableTs).isEqualTo(response2.getImmutableTimestamp());

        namespace.unlock(response2.getLock());
    }

    @Test
    public void batchedStartTransactionCallShouldLockImmutableTimestamp() {
        // requestor id corresponds to an instance of the timelock service client
        ConjureStartTransactionsRequest request = ConjureStartTransactionsRequest.builder()
                .numTransactions(123)
                .requestorId(UUID.randomUUID())
                .requestId(UUID.randomUUID())
                .lastKnownVersion(Optional.empty())
                .build();

        LockImmutableTimestampResponse response1 = namespace
                .namespacedConjureTimelockService()
                .startTransactions(request)
                .getImmutableTimestamp();

        LockImmutableTimestampResponse response2 = namespace
                .namespacedConjureTimelockService()
                .startTransactions(request)
                .getImmutableTimestamp();

        // above are two *separate* batches

        long immutableTs = namespace.timelockService().getImmutableTimestamp();
        assertThat(immutableTs).isEqualTo(response1.getImmutableTimestamp());

        namespace
                .namespacedConjureTimelockService()
                .unlock(ConjureUnlockRequest.of(
                        ImmutableSet.of(ConjureLockToken.of(response1.getLock().getRequestId()))));

        assertThat(immutableTs).isEqualTo(response2.getImmutableTimestamp());

        namespace
                .namespacedConjureTimelockService()
                .unlock(ConjureUnlockRequest.of(
                        ImmutableSet.of(ConjureLockToken.of(response2.getLock().getRequestId()))));
    }

    @Test
    public void immutableTimestampIsGreaterThanFreshTimestampWhenNotLocked() {
        long freshTs = namespace.getFreshTimestamp();
        long immutableTs = namespace.timelockService().getImmutableTimestamp();
        assertThat(immutableTs).isGreaterThan(freshTs);
    }

    @Test
    public void taggedMetricsCanBeIteratedThrough() {
        NamespacedClients randomNamespace = cluster.clientForRandomNamespace();
        randomNamespace.getFreshTimestamp();
        Map<MetricName, Metric> metrics = cluster.currentLeaderFor(randomNamespace.namespace())
                .taggedMetricRegistry()
                .getMetrics();
        assertThat(metrics).hasKeySatisfying(new Condition<MetricName>("contains random namespace") {
            @Override
            public boolean matches(MetricName value) {
                return randomNamespace.namespace().equals(value.safeTags().get("client"));
            }
        });
    }

    @Test
    public void cannotRetrieveImmutableTimestampViaSyncApiForAsyncService() {
        if (cluster != CLUSTER_WITH_ASYNC) {
            // safety check only applies if the cluster is indeed async
            return;
        }
        // Catching any exception since this currently is an error deserialization exception
        // until we stop requiring http-remoting2 errors
        assertThatThrownBy(() -> namespace.legacyLockService().getMinLockedInVersionId("foo"))
                .isInstanceOf(Exception.class);
    }

    @Test
    public void canRetrieveImmutableTimestampViaSyncApiForSyncServiceOrAsyncWithoutSafetyCheck() {
        if (cluster == CLUSTER_WITH_ASYNC) {
            // will fail - ensuring this fails is covered by cannotRetrieveImmutableTimestampViaSyncApiForAsyncService
            return;
        }
        namespace.legacyLockService().getMinLockedInVersionId("foo");
    }

    @Test
    public void canWaitForLocks() {
        LockToken token = namespace.lock(requestFor(LOCK_A, LOCK_B)).getToken();

        CompletableFuture<WaitForLocksResponse> future = waitForLocksAsync(waitRequestFor(LOCK_A, LOCK_B));
        assertNotDone(future);

        namespace.unlock(token);

        assertDone(future);
        assertThat(future.join().wasSuccessful()).isTrue();
    }

    private CompletableFuture<WaitForLocksResponse> waitForLocksAsync(WaitForLocksRequest waitForLocksRequest) {
        return CompletableFuture.supplyAsync(() -> namespace.waitForLocks(waitForLocksRequest), executor);
    }

    @Test
    public void canGetTimestamps() {
        List<Long> timestamps = ImmutableList.of(
                namespace.getFreshTimestamp(),
                namespace.timelockService().getFreshTimestamp(),
                namespace.getFreshTimestamp(),
                namespace.timelockService().getFreshTimestamp());

        assertThat(timestamps).isSorted();
    }

    @Test
    public void canGetBatchTimestamps() {
        TimestampRange range1 = namespace.getFreshTimestamps(5);
        TimestampRange range2 = namespace.getFreshTimestamps(5);

        assertThat(range1.getUpperBound()).isLessThan(range2.getLowerBound());
    }

    @Test
    public void lockRequestCanTimeOut() {
        LockToken token = namespace.lock(requestFor(LOCK_A)).getToken();
        LockResponse token2 = namespace.lock(requestFor(SHORT_TIMEOUT, LOCK_A));

        assertThat(token2.wasSuccessful()).isFalse();
        namespace.unlock(token);
    }

    @Test
    public void waitForLocksRequestCanTimeOut() {
        LockToken token = namespace.lock(requestFor(LOCK_A)).getToken();
        WaitForLocksResponse response = namespace.waitForLocks(waitRequestFor(SHORT_TIMEOUT, LOCK_A));

        assertThat(response.wasSuccessful()).isFalse();
        namespace.unlock(token);
    }

    @Test
    public void canGetCurrentTimeMillis() {
        assertThat(namespace.legacyLockService().currentTimeMillis()).isGreaterThan(0L);
        assertThat(namespace.timelockService().currentTimeMillis()).isGreaterThan(0L);
    }

    @Test
    public void canPerformLockAndUnlock() throws InterruptedException {
        HeldLocksToken token1 = lockWithFullResponse(requestForWriteLock(LOCK_A), TEST_CLIENT);
        namespace.legacyLockService().unlock(token1);
        HeldLocksToken token2 = lockWithFullResponse(requestForWriteLock(LOCK_A), TEST_CLIENT);
        namespace.legacyLockService().unlockSimple(SimpleHeldLocksToken.fromHeldLocksToken(token2));
    }

    @Test
    public void canPerformLockAndRefreshLock() throws InterruptedException {
        HeldLocksToken token1 = lockWithFullResponse(requestForWriteLock(LOCK_A), TEST_CLIENT);
        LockRefreshToken token = token1.getLockRefreshToken();
        Set<LockRefreshToken> lockRefreshTokens =
                namespace.legacyLockService().refreshLockRefreshTokens(ImmutableList.of(token));
        assertThat(lockRefreshTokens).contains(token);

        unlock(token1);
    }

    @Test
    public void unlockAndFreezeDoesNotAllowRefreshes() throws InterruptedException {
        HeldLocksToken token = lockWithFullResponse(requestForWriteLock(LOCK_A), TEST_CLIENT);
        namespace.legacyLockService().unlockAndFreeze(token);
        Set<LockRefreshToken> lockRefreshTokens =
                namespace.legacyLockService().refreshLockRefreshTokens(ImmutableList.of(token.getLockRefreshToken()));
        assertThat(lockRefreshTokens).isEmpty();
    }

    @Test
    public void testGetAndRefreshTokens() throws InterruptedException {
        HeldLocksToken token1 = lockWithFullResponse(requestForWriteLock(LOCK_A), TEST_CLIENT);
        HeldLocksToken token2 = lockWithFullResponse(requestForWriteLock(LOCK_B), TEST_CLIENT);

        Set<HeldLocksToken> tokens = namespace.legacyLockService().getTokens(TEST_CLIENT);
        assertThat(tokens.stream()
                        .map(token -> Iterables.getOnlyElement(token.getLocks()).getLockDescriptor())
                        .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(LOCK_A, LOCK_B);

        Set<HeldLocksToken> heldLocksTokens = namespace.legacyLockService().refreshTokens(tokens);
        assertThat(heldLocksTokens.stream()
                        .map(token -> Iterables.getOnlyElement(token.getLocks()).getLockDescriptor())
                        .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(LOCK_A, LOCK_B);

        unlock(token1, token2);
    }

    @Test
    public void lockGrantsCanBeServedAndRefreshed() throws InterruptedException {
        HeldLocksToken heldLocksToken = lockWithFullResponse(requestForReadLock(LOCK_A), TEST_CLIENT);
        HeldLocksGrant heldLocksGrant = namespace.legacyLockService().convertToGrant(heldLocksToken);

        assertThat(namespace.legacyLockService().getTokens(TEST_CLIENT)).isEmpty();

        HeldLocksGrant refreshedLockGrant = namespace.legacyLockService().refreshGrant(heldLocksGrant);
        assertThat(refreshedLockGrant).isEqualTo(heldLocksGrant);

        HeldLocksGrant refreshedLockGrant2 = namespace.legacyLockService().refreshGrant(heldLocksGrant.getGrantId());
        assertThat(refreshedLockGrant2).isEqualTo(heldLocksGrant);

        namespace.legacyLockService().useGrant(TEST_CLIENT_2, heldLocksGrant);

        SortedLockCollection<LockDescriptor> lockDescriptors = Iterables.getOnlyElement(
                        namespace.legacyLockService().getTokens(TEST_CLIENT_2))
                .getLockDescriptors();
        assertThat(lockDescriptors).contains(LOCK_A);

        // Catching any exception since this currently is an error deserialization exception
        // until we stop requiring http-remoting2 errors
        assertThatThrownBy(() -> namespace.legacyLockService().useGrant(TEST_CLIENT_3, heldLocksGrant.getGrantId()))
                .isInstanceOf(Exception.class);
    }

    @Test
    public void getMinLockedInVersionIdReturnsNullIfNoVersionIdsAreSpecified() throws InterruptedException {
        HeldLocksToken token = lockWithFullResponse(requestForReadLock(LOCK_A), TEST_CLIENT);
        assertThat(namespace.legacyLockService().getMinLockedInVersionId(TEST_CLIENT))
                .isNull();
        unlock(token);
    }

    @Test
    public void getMinLockedInVersionIdReturnsValidValues() throws InterruptedException {
        HeldLocksToken token1 = lockWithFullResponse(requestForReadLock(LOCK_A, 10L), TEST_CLIENT);
        HeldLocksToken token2 = lockWithFullResponse(requestForReadLock(LOCK_B, 12L), TEST_CLIENT);
        assertThat(namespace.legacyLockService().getMinLockedInVersionId(TEST_CLIENT))
                .isEqualTo(10L);
        unlock(token1, token2);
    }

    @Test
    public void getMinLockedInVersionIdReturnsValidValuesForAnonymousClient() throws InterruptedException {
        HeldLocksToken token1 = lockWithFullResponse(requestForReadLock(LOCK_A, 10L), LockClient.ANONYMOUS);
        HeldLocksToken token2 = lockWithFullResponse(requestForReadLock(LOCK_B, 12L), LockClient.ANONYMOUS);
        assertThat(namespace.legacyLockService().getMinLockedInVersionId()).isEqualTo(10L);
        unlock(token1, token2);
    }

    @Test
    public void testGetLockServerOptions() {
        assertThat(namespace.legacyLockService().getLockServerOptions())
                .isEqualTo(LockServerOptions.builder().randomBitCount(127).build());
    }

    @Test
    public void testCurrentTimeMillis() {
        assertThat(namespace.legacyLockService().currentTimeMillis()).isPositive();
    }

    @Test
    public void lockRequestsToRpcClientAreIdempotent() {
        LockToken token = namespace.lock(requestFor(LOCK_A)).getToken();

        ConjureLockRequest secondRequest = requestFor(CONJURE_LOCK_A);
        CompletableFuture<ConjureLockResponse> responseFuture = lockWithRpcClientAsync(secondRequest);
        CompletableFuture<ConjureLockResponse> duplicateResponseFuture = lockWithRpcClientAsync(secondRequest);

        namespace.unlock(token);

        ConjureLockResponse response = responseFuture.join();
        ConjureLockResponse duplicateResponse = duplicateResponseFuture.join();
        assertThat(response).isEqualTo(duplicateResponse);

        namespace
                .namespacedConjureTimelockService()
                .unlock(ConjureUnlockRequest.of(ImmutableSet.of(getToken(response))));
    }

    private CompletableFuture<ConjureLockResponse> lockWithRpcClientAsync(ConjureLockRequest lockRequest) {
        return CompletableFuture.supplyAsync(
                () -> namespace.namespacedConjureTimelockService().lock(lockRequest), executor);
    }

    private static ConjureLockToken getToken(ConjureLockResponse response) {
        return response.accept(new ConjureLockResponse.Visitor<ConjureLockToken>() {
            @Override
            public ConjureLockToken visitSuccessful(SuccessfulLockResponse value) {
                return value.getLockToken();
            }

            @Override
            public ConjureLockToken visitUnsuccessful(UnsuccessfulLockResponse value) {
                throw new RuntimeException("Unsuccessful lock request");
            }

            @Override
            public ConjureLockToken visitUnknown(String unknownType) {
                throw new RuntimeException("Unknown");
            }
        });
    }

    @Test
    public void lockSucceedsOnlyOnce() {
        LockRequest request = requestFor(SHORT_TIMEOUT, LOCK_A);

        LockResponse response = namespace.lock(request);
        LockResponse duplicateResponse = namespace.lock(request);

        assertThat(response.wasSuccessful()).isTrue();
        assertThat(duplicateResponse.wasSuccessful()).isFalse();

        namespace.unlock(response.getToken());
    }

    @Test
    public void waitForLockRequestsAreIdempotent() {
        LockToken token = namespace.lock(requestFor(LOCK_A)).getToken();

        WaitForLocksRequest request = waitRequestFor(LOCK_A);
        CompletableFuture<WaitForLocksResponse> response = waitForLocksAsync(request);
        CompletableFuture<WaitForLocksResponse> duplicateResponse = waitForLocksAsync(request);

        namespace.unlock(token);

        assertThat(response.join()).isEqualTo(duplicateResponse.join());
    }

    @Test
    public void startIdentifiedAtlasDbTransactionGivesUsTimestampsInSequence() {
        StartIdentifiedAtlasDbTransactionResponse firstResponse = startSingleTransaction(namespace.timelockService());
        StartIdentifiedAtlasDbTransactionResponse secondResponse = startSingleTransaction(namespace.timelockService());

        assertThatStartIdentifiedTransactionResponseTimestampsInSequence(firstResponse, secondResponse);
    }

    @Test
    public void startIdentifiedAtlasDbTransactionBatchGivesUsTimestampsInSequence() {
        List<StartIdentifiedAtlasDbTransactionResponse> responses =
                namespace.timelockService().startIdentifiedAtlasDbTransactionBatch(2);

        assertThatStartIdentifiedTransactionResponseTimestampsInSequence(responses.get(0), responses.get(1));
    }

    @Test
    public void startIdentifiedAtlasDbTransactionGivesUsStartTimestampsInTheSamePartition() {
        StartIdentifiedAtlasDbTransactionResponse firstResponse = startSingleTransaction(namespace.timelockService());
        StartIdentifiedAtlasDbTransactionResponse secondResponse = startSingleTransaction(namespace.timelockService());

        assertThatStartIdentifiedTransactionResponseTimestampsInSamePartition(firstResponse, secondResponse);
    }

    @Test
    public void startIdentifiedAtlasDbTransactionBatchGivesUsStartTimestampsInTheSamePartition() {
        List<StartIdentifiedAtlasDbTransactionResponse> responses =
                namespace.timelockService().startIdentifiedAtlasDbTransactionBatch(2);

        assertThatStartIdentifiedTransactionResponseTimestampsInSamePartition(responses.get(0), responses.get(1));
    }

    @Test
    public void temporalOrderingIsPreservedWhenMixingStandardTimestampAndIdentifiedTimestampRequests() {
        List<Long> temporalSequence = ImmutableList.of(
                namespace.getFreshTimestamp(),
                startSingleTransaction(namespace.timelockService())
                        .startTimestampAndPartition()
                        .timestamp(),
                namespace.getFreshTimestamp(),
                startSingleTransaction(namespace.timelockService())
                        .startTimestampAndPartition()
                        .timestamp(),
                namespace.getFreshTimestamp());

        assertThat(temporalSequence).isSorted();
    }

    @Test
    public void distinctClientsForTheSameNamespaceStillShareTheSameSequenceOfTimestamps() {
        TimelockService independentClient1 =
                cluster.uncachedNamespacedClients(namespace.namespace()).timelockService();
        TimelockService independentClient2 =
                cluster.uncachedNamespacedClients(namespace.namespace()).timelockService();

        List<Long> temporalSequence = ImmutableList.of(
                startSingleTransaction(independentClient1)
                        .startTimestampAndPartition()
                        .timestamp(),
                startSingleTransaction(independentClient1)
                        .startTimestampAndPartition()
                        .timestamp(),
                startSingleTransaction(independentClient2)
                        .startTimestampAndPartition()
                        .timestamp(),
                startSingleTransaction(independentClient2)
                        .startTimestampAndPartition()
                        .timestamp(),
                startSingleTransaction(independentClient1)
                        .startTimestampAndPartition()
                        .timestamp(),
                startSingleTransaction(independentClient2)
                        .startTimestampAndPartition()
                        .timestamp(),
                startSingleTransaction(independentClient1)
                        .startTimestampAndPartition()
                        .timestamp());

        assertThat(temporalSequence).isSorted();
    }

    @Test
    public void temporalOrderingIsPreservedForBatchedStartTransactionRequests() {
        UUID requestor = UUID.randomUUID();
        List<Long> allTimestamps = new ArrayList<>();

        allTimestamps.addAll(getSortedBatchedStartTimestamps(requestor, 1));
        allTimestamps.addAll(getSortedBatchedStartTimestamps(requestor, 4));
        allTimestamps.addAll(getSortedBatchedStartTimestamps(requestor, 20));

        assertThat(allTimestamps).isSorted();
    }

    @Test
    public void temporalOrderingIsPreservedBetweenDifferentRequestorsForBatchedStartTransactionRequests() {
        UUID requestor = UUID.randomUUID();
        UUID requestor2 = UUID.randomUUID();
        List<Long> allTimestamps = new ArrayList<>();

        allTimestamps.addAll(getSortedBatchedStartTimestamps(requestor, 1));
        allTimestamps.addAll(getSortedBatchedStartTimestamps(requestor2, 4));
        allTimestamps.addAll(getSortedBatchedStartTimestamps(requestor, 20));
        allTimestamps.addAll(getSortedBatchedStartTimestamps(requestor2, 15));

        assertThat(allTimestamps).isSorted();
    }

    @Test
    public void batchedTimestampsShouldBeSeparatedByModulus() {
        UUID requestor = UUID.randomUUID();

        List<Long> sortedTimestamps = getSortedBatchedStartTimestamps(requestor, 10);

        Set<Long> differences = IntStream.range(0, sortedTimestamps.size() - 1)
                .mapToObj(i -> sortedTimestamps.get(i + 1) - sortedTimestamps.get(i))
                .collect(Collectors.toSet());

        assertThat(differences).containsOnly((long) TransactionConstants.V2_TRANSACTION_NUM_PARTITIONS);
    }

    private StartIdentifiedAtlasDbTransactionResponse startSingleTransaction(TimelockService timelockService) {
        return Iterables.getOnlyElement(timelockService.startIdentifiedAtlasDbTransactionBatch(1));
    }

    private static void assertThatStartIdentifiedTransactionResponseTimestampsInSequence(
            StartIdentifiedAtlasDbTransactionResponse firstResponse,
            StartIdentifiedAtlasDbTransactionResponse secondResponse) {
        // Note that we technically cannot guarantee an ordering between the fresh timestamp on response 1 and the
        // immutable timestamp on response 2. Most of the time, we will have IT on response 2 = IT on response 1
        // < FT on response 1, as the lock token on response 1 has not expired yet. However, if we sleep for long
        // enough between the first and second call that the immutable timestamp lock expires, then
        // IT on response 2 > FT on response 1.
        assertThat(ImmutableList.of(
                        firstResponse.immutableTimestamp().getImmutableTimestamp(),
                        firstResponse.startTimestampAndPartition().timestamp(),
                        secondResponse.startTimestampAndPartition().timestamp()))
                .isSorted();
        assertThat(ImmutableList.of(
                        firstResponse.immutableTimestamp().getImmutableTimestamp(),
                        secondResponse.immutableTimestamp().getImmutableTimestamp(),
                        secondResponse.startTimestampAndPartition().timestamp()))
                .isSorted();
    }

    private void assertThatStartIdentifiedTransactionResponseTimestampsInSamePartition(
            StartIdentifiedAtlasDbTransactionResponse firstResponse,
            StartIdentifiedAtlasDbTransactionResponse secondResponse) {
        assertThat(firstResponse.startTimestampAndPartition().partition())
                .isEqualTo(secondResponse.startTimestampAndPartition().partition());
    }

    private HeldLocksToken lockWithFullResponse(com.palantir.lock.LockRequest request, LockClient client)
            throws InterruptedException {
        return namespace
                .legacyLockService()
                .lockWithFullLockResponse(client, request)
                .getToken();
    }

    private static com.palantir.lock.LockRequest requestForReadLock(LockDescriptor lock) {
        return com.palantir.lock.LockRequest.builder(ImmutableSortedMap.of(lock, LockMode.READ))
                .timeoutAfter(SimpleTimeDuration.of(10, TimeUnit.MILLISECONDS))
                .build();
    }

    private static com.palantir.lock.LockRequest requestForReadLock(LockDescriptor lockA, long versionId) {
        return com.palantir.lock.LockRequest.builder(ImmutableSortedMap.of(lockA, LockMode.READ))
                .withLockedInVersionId(versionId)
                .timeoutAfter(SimpleTimeDuration.of(10, TimeUnit.MILLISECONDS))
                .build();
    }

    private static com.palantir.lock.LockRequest requestForWriteLock(LockDescriptor lock) {
        return com.palantir.lock.LockRequest.builder(ImmutableSortedMap.of(lock, LockMode.WRITE))
                .timeoutAfter(SimpleTimeDuration.of(10, TimeUnit.MILLISECONDS))
                .build();
    }

    private static ConjureLockRequest requestFor(ConjureLockDescriptor... locks) {
        return ConjureLockRequest.builder()
                .lockDescriptors(ImmutableSet.copyOf(locks))
                .acquireTimeoutMs(TIMEOUT)
                .requestId(UUID.randomUUID())
                .build();
    }

    private static LockRequest requestFor(LockDescriptor... locks) {
        return LockRequest.of(ImmutableSet.copyOf(locks), TIMEOUT);
    }

    private static LockRequest requestFor(long timeoutMs, LockDescriptor... locks) {
        return LockRequest.of(ImmutableSet.copyOf(locks), timeoutMs);
    }

    private static WaitForLocksRequest waitRequestFor(LockDescriptor... locks) {
        return WaitForLocksRequest.of(ImmutableSet.copyOf(locks), TIMEOUT);
    }

    private static WaitForLocksRequest waitRequestFor(long timeoutMs, LockDescriptor... locks) {
        return WaitForLocksRequest.of(ImmutableSet.copyOf(locks), timeoutMs);
    }

    private static void assertNotYetLocked(Future<LockToken> futureToken) {
        assertNotDone(futureToken);
    }

    private void assertLockedAndUnlock(Future<LockToken> futureToken) {
        namespace.unlock(assertLocked(futureToken));
    }

    private static LockToken assertLocked(Future<LockToken> futureToken) {
        return assertDone(futureToken);
    }

    private static void assertNotDone(Future<?> future) {
        assertThatThrownBy(() -> future.get(1, TimeUnit.SECONDS)).isInstanceOf(TimeoutException.class);
    }

    private static <T> T assertDone(Future<T> future) {
        try {
            return future.get(1, TimeUnit.SECONDS);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private void unlock(HeldLocksToken... tokens) {
        for (HeldLocksToken token : tokens) {
            namespace.legacyLockService().unlock(token);
        }
    }

    private List<Long> getSortedBatchedStartTimestamps(UUID requestorUuid, int numRequestedTimestamps) {
        ConjureStartTransactionsRequest request = ConjureStartTransactionsRequest.builder()
                .requestId(UUID.randomUUID())
                .requestorId(requestorUuid)
                .numTransactions(numRequestedTimestamps)
                .lastKnownVersion(Optional.empty())
                .build();
        ConjureStartTransactionsResponse response =
                namespace.namespacedConjureTimelockService().startTransactions(request);
        return response.getTimestamps().stream().boxed().collect(Collectors.toList());
    }
}
