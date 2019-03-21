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
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.timelock.util.ExceptionMatchers;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionRequest;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.StartTransactionRequestV4;
import com.palantir.lock.v2.StartTransactionResponseV4;
import com.palantir.lock.v2.TimelockService;

public class MultiNodePaxosTimeLockServerIntegrationTest {
    private static final String CLIENT_2 = "test2";
    private static final String CLIENT_3 = "test3";
    private static final List<String> ADDITIONAL_CLIENTS = ImmutableList.of(CLIENT_2, CLIENT_3);

    private static final TestableTimelockCluster CLUSTER = new TestableTimelockCluster(
            "https://localhost",
            "paxosMultiServer0.yml",
            "paxosMultiServer1.yml",
            "paxosMultiServer2.yml");

    private static final LockDescriptor LOCK = StringLockDescriptor.of("foo");
    private static final Set<LockDescriptor> LOCKS = ImmutableSet.of(LOCK);

    private static final com.palantir.lock.LockRequest BLOCKING_LOCK_REQUEST = com.palantir.lock.LockRequest.builder(
            ImmutableSortedMap.of(
                    StringLockDescriptor.of("foo"),
                    LockMode.WRITE))
            .build();
    private static final int DEFAULT_LOCK_TIMEOUT_MS = 10_000;

    @ClassRule
    public static final RuleChain ruleChain = CLUSTER.getRuleChain();

    @BeforeClass
    public static void waitForClusterToStabilize() {
        CLUSTER.waitUntilLeaderIsElected(ADDITIONAL_CLIENTS);
    }

    @Before
    public void bringAllNodesOnline() {
        CLUSTER.waitUntilAllServersOnlineAndReadyToServeClients(ADDITIONAL_CLIENTS);
    }

    @Test
    public void blockedLockRequestThrows503OnLeaderElectionForRemoteLock() throws InterruptedException {
        LockRefreshToken lock = CLUSTER.remoteLock(BLOCKING_LOCK_REQUEST);
        assertThat(lock).isNotNull();

        TestableTimelockServer leader = CLUSTER.currentLeader();

        CompletableFuture<LockRefreshToken> lockRefreshTokenCompletableFuture = CompletableFuture.supplyAsync(() -> {
            try {
                return leader.remoteLock(CLIENT_2, BLOCKING_LOCK_REQUEST);
            } catch (InterruptedException e) {
                return null;
            }
        });
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        CLUSTER.nonLeaders().forEach(TestableTimelockServer::kill);
        // Lock on leader so that AwaitingLeadershipProxy notices leadership loss.
        assertThatThrownBy(() -> leader.remoteLock(CLIENT_3, BLOCKING_LOCK_REQUEST))
                .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);

        assertThat(catchThrowable(lockRefreshTokenCompletableFuture::get).getCause())
                .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);
    }

    @Test
    public void blockedLockRequestThrows503OnLeaderElectionForAsyncLock() {
        CLUSTER.lock(LockRequest.of(LOCKS, DEFAULT_LOCK_TIMEOUT_MS)).getToken();

        TestableTimelockServer leader = CLUSTER.currentLeader();

        CompletableFuture<LockResponse> token2 = CompletableFuture.supplyAsync(
                () -> leader.lock(LockRequest.of(LOCKS, 60_000)));

        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        CLUSTER.nonLeaders().forEach(TestableTimelockServer::kill);
        // Lock on leader so that AwaitingLeadershipProxy notices leadership loss.
        assertThatThrownBy(() -> leader.lock(LockRequest.of(LOCKS, DEFAULT_LOCK_TIMEOUT_MS)))
                .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);

        assertThat(catchThrowable(token2::get).getCause())
                .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);
    }

    @Test
    public void nonLeadersReturn503() {
        CLUSTER.nonLeaders().forEach(server -> {
            assertThatThrownBy(server::getFreshTimestamp)
                    .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);
            assertThatThrownBy(() -> server.lock(LockRequest.of(LOCKS, DEFAULT_LOCK_TIMEOUT_MS)))
                    .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);
        });
    }

    @Test
    public void leaderRespondsToRequests() {
        CLUSTER.currentLeader().getFreshTimestamp();

        LockToken token = CLUSTER.currentLeader().lock(LockRequest.of(LOCKS, DEFAULT_LOCK_TIMEOUT_MS)).getToken();
        CLUSTER.unlock(token);
    }

    @Test
    public void newLeaderTakesOverIfCurrentLeaderDies() {
        CLUSTER.currentLeader().kill();

        CLUSTER.getFreshTimestamp();
    }

    @Test
    public void leaderLosesLeadershipIfQuorumIsNotAlive() {
        TestableTimelockServer leader = CLUSTER.currentLeader();
        CLUSTER.nonLeaders().forEach(TestableTimelockServer::kill);

        assertThatThrownBy(leader::getFreshTimestamp)
                .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);
    }

    @Test
    public void someoneBecomesLeaderAgainAfterQuorumIsRestored() {
        CLUSTER.nonLeaders().forEach(TestableTimelockServer::kill);
        CLUSTER.nonLeaders().forEach(TestableTimelockServer::start);

        CLUSTER.getFreshTimestamp();
    }

    @Test
    public void canPerformRollingRestart() {
        bringAllNodesOnline();
        for (TestableTimelockServer server : CLUSTER.servers()) {
            server.kill();
            waitForClusterToStabilize();
            CLUSTER.getFreshTimestamp();
            server.start();
        }
    }

    @Test
    public void timestampsAreIncreasingAcrossFailovers() {
        long lastTimestamp = CLUSTER.getFreshTimestamp();

        for (int i = 0; i < 3; i++) {
            CLUSTER.failoverToNewLeader();

            long timestamp = CLUSTER.getFreshTimestamp();
            assertThat(timestamp).isGreaterThan(lastTimestamp);
            lastTimestamp = timestamp;
        }
    }

    @Test
    public void leaderIdChangesAcrossFailovers() {
        Set<LeaderTime> leaderTimes = new HashSet<>();
        leaderTimes.add(CLUSTER.timelockRpcClient().getLeaderTime());

        for (int i = 0; i < 3; i++) {
            CLUSTER.failoverToNewLeader();

            LeaderTime leaderTime = CLUSTER.timelockRpcClient().getLeaderTime();
            leaderTimes.forEach(previousLeaderTime ->
                    assertThat(previousLeaderTime.isComparableWith(leaderTime)).isFalse());
            leaderTimes.add(leaderTime);
        }
    }

    @Test
    public void locksAreInvalidatedAcrossFailures() {
        LockToken token = CLUSTER.lock(LockRequest.of(LOCKS, DEFAULT_LOCK_TIMEOUT_MS)).getToken();

        for (int i = 0; i < 3; i++) {
            CLUSTER.failoverToNewLeader();

            assertThat(CLUSTER.unlock(token)).isFalse();
            token = CLUSTER.lock(LockRequest.of(LOCKS, DEFAULT_LOCK_TIMEOUT_MS)).getToken();
        }
    }

    @Test
    public void canCreateNewClientsDynamically() {
        for (int i = 0; i < 5; i++) {
            String client = UUID.randomUUID().toString();
            TimelockService timelock = CLUSTER.timelockServiceForClient(client);

            timelock.getFreshTimestamp();
            LockToken token = timelock.lock(LockRequest.of(LOCKS, DEFAULT_LOCK_TIMEOUT_MS)).getToken();
            CLUSTER.unlock(token);
        }
    }

    @Test
    public void clientsCreatedDynamicallyOnNonLeadersAreFunctionalAfterFailover() {
        String client = UUID.randomUUID().toString();
        CLUSTER.nonLeaders().forEach(server -> {
            assertThatThrownBy(() -> server.timelockServiceForClient(client).getFreshTimestamp())
                    .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);
        });

        CLUSTER.failoverToNewLeader();

        CLUSTER.getFreshTimestamp();
    }

    @Test
    public void clientsCreatedDynamicallyOnLeaderAreFunctionalImmediately() {
        String client = UUID.randomUUID().toString();

        CLUSTER.currentLeader()
                .timelockServiceForClient(client)
                .getFreshTimestamp();
    }

    @Test
    public void noConflictIfLeaderAndNonLeadersSeparatelyInitializeClient() {
        String client = UUID.randomUUID().toString();
        CLUSTER.nonLeaders().forEach(server -> {
            assertThatThrownBy(() -> server.timelockServiceForClient(client).getFreshTimestamp())
                    .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);
        });

        long ts1 = CLUSTER.timelockServiceForClient(client).getFreshTimestamp();

        CLUSTER.failoverToNewLeader();

        long ts2 = CLUSTER.getFreshTimestamp();
        assertThat(ts1).isLessThan(ts2);
    }

    @Test
    public void clockSkewMetricsSmokeTest() {
        Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);

        for (TestableTimelockServer server : CLUSTER.servers()) {
            MetricsOutput metrics = server.getMetricsOutput();

            metrics.assertContainsHistogram("clock.skew");
            assertThat(metrics.getHistogram("clock.skew").get("count").intValue()).isGreaterThan(0);
        }
    }

    @Test
    public void startIdentifiedAtlasDbTransactionGivesUsTimestampsInSequence() {
        UUID requestorUuid = UUID.randomUUID();
        StartIdentifiedAtlasDbTransactionResponse firstResponse = startIdentifiedAtlasDbTransaction(requestorUuid);
        StartIdentifiedAtlasDbTransactionResponse secondResponse = startIdentifiedAtlasDbTransaction(requestorUuid);

        // Note that we technically cannot guarantee an ordering between the fresh timestamp on response 1 and the
        // immutable timestamp on response 2. Most of the time, we will have IT on response 2 = IT on response 1
        // < FT on response 1, as the lock token on response 1 has not expired yet. However, if we sleep for long
        // enough between the first and second call that the immutable timestamp lock expires, then
        // IT on response 2 > FT on response 1.
        assertThat(ImmutableList.of(
                firstResponse.immutableTimestamp().getImmutableTimestamp(),
                firstResponse.startTimestampAndPartition().timestamp(),
                secondResponse.startTimestampAndPartition().timestamp())).isSorted();
        assertThat(ImmutableList.of(
                firstResponse.immutableTimestamp().getImmutableTimestamp(),
                secondResponse.immutableTimestamp().getImmutableTimestamp(),
                secondResponse.startTimestampAndPartition().timestamp())).isSorted();
    }

    @Test
    public void startIdentifiedAtlasDbTransactionGivesUsStartTimestampsInTheSamePartition() {
        UUID requestorUuid = UUID.randomUUID();
        StartIdentifiedAtlasDbTransactionResponse firstResponse = startIdentifiedAtlasDbTransaction(requestorUuid);
        StartIdentifiedAtlasDbTransactionResponse secondResponse = startIdentifiedAtlasDbTransaction(requestorUuid);

        assertThat(firstResponse.startTimestampAndPartition().partition())
                .isEqualTo(secondResponse.startTimestampAndPartition().partition());
    }

    @Test
    public void temporalOrderingIsPreservedWhenMixingStandardTimestampAndIdentifiedTimestampRequests() {
        UUID requestorUuid = UUID.randomUUID();
        List<Long> temporalSequence = ImmutableList.of(
                CLUSTER.getFreshTimestamp(),
                getStartTimestampFromIdentifiedAtlasDbTransaction(requestorUuid),
                CLUSTER.getFreshTimestamp(),
                getStartTimestampFromIdentifiedAtlasDbTransaction(requestorUuid),
                CLUSTER.getFreshTimestamp());

        assertThat(temporalSequence).isSorted();
    }

    @Test
    public void distinctClientsStillShareTheSameSequenceOfTimestamps() {
        UUID requestor1 = UUID.randomUUID();
        UUID requestor2 = UUID.randomUUID();

        List<Long> temporalSequence = ImmutableList.of(
                getStartTimestampFromIdentifiedAtlasDbTransaction(requestor1),
                getStartTimestampFromIdentifiedAtlasDbTransaction(requestor1),
                getStartTimestampFromIdentifiedAtlasDbTransaction(requestor2),
                getStartTimestampFromIdentifiedAtlasDbTransaction(requestor2),
                getStartTimestampFromIdentifiedAtlasDbTransaction(requestor1),
                getStartTimestampFromIdentifiedAtlasDbTransaction(requestor2),
                getStartTimestampFromIdentifiedAtlasDbTransaction(requestor1));

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

    private List<Long> getSortedBatchedStartTimestamps(UUID requestorUuid, int numRequestedTimestamps) {
        StartTransactionRequestV4 request = StartTransactionRequestV4.createForRequestor(
                requestorUuid,
                numRequestedTimestamps);
        StartTransactionResponseV4 response = CLUSTER.timelockRpcClient().startTransactions(request);
        return response.timestamps().stream()
                .boxed()
                .collect(Collectors.toList());
    }

    private static StartIdentifiedAtlasDbTransactionResponse startIdentifiedAtlasDbTransaction(UUID requestorUuid) {
        return CLUSTER.startIdentifiedAtlasDbTransaction(
                StartIdentifiedAtlasDbTransactionRequest.createForRequestor(requestorUuid));
    }

    private static long getStartTimestampFromIdentifiedAtlasDbTransaction(UUID requestorUuid) {
        return startIdentifiedAtlasDbTransaction(requestorUuid).startTimestampAndPartition().timestamp();
    }
}
