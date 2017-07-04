/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.timelock.util.ExceptionMatchers;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.StringLockDescriptor;

import feign.RetryableException;

public class MultiNodePaxosTimeLockServerIntegrationTest {
    private static final String CLIENT_1 = "test";
    private static final String CLIENT_2 = "test2";
    private static final String CLIENT_3 = "test3";

    private static final TestableTimelockCluster CLUSTER = new TestableTimelockCluster(
            "http://localhost",
            CLIENT_1,
            "paxosMultiServer0.yml",
            "paxosMultiServer1.yml",
            "paxosMultiServer2.yml");

    private static final String LOCK_CLIENT = LockClient.ANONYMOUS.getClientId();
    private static final LockRequest LOCK_REQUEST = LockRequest.builder(
            ImmutableSortedMap.of(
                    StringLockDescriptor.of("lock1"),
                    LockMode.WRITE))
            .doNotBlock()
            .build();

    private static final LockRequest BLOCKING_LOCK_REQUEST = LockRequest.builder(
            ImmutableSortedMap.of(
                    StringLockDescriptor.of("foo"),
                    LockMode.WRITE))
            .build();

    @ClassRule
    public static final RuleChain ruleChain = CLUSTER.getRuleChain();

    @BeforeClass
    public static void waitForClusterToStabilize() {
        CLUSTER.waitUntilLeaderIsElected();
    }

    @Before
    public void bringAllNodesOnline() {
        CLUSTER.waitUntillAllSeversAreOnlineAndLeaderIsElected();
    }

    @Test
    public void lockRequestThrows500DueToLeaderElection() throws InterruptedException {
        CLUSTER.lock(CLIENT_1, BLOCKING_LOCK_REQUEST);

        TestableTimelockServer leader = CLUSTER.currentLeader();
        Executors.newSingleThreadExecutor().submit(() -> {
            assertThatThrownBy(() -> leader.lock(CLIENT_2, BLOCKING_LOCK_REQUEST))
                    .isInstanceOf(InterruptedException.class);
        });

        Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
        CLUSTER.nonLeaders().forEach(TestableTimelockServer::kill);
        // Lock on leader so that AwaitingLeadershipProxy notices leadership loss.
        assertThatThrownBy(() -> leader.lock(CLIENT_3, BLOCKING_LOCK_REQUEST))
                .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);
    }

    @Test
    public void lockRequestRetriesAfter500DueToLeaderElection() throws InterruptedException {
        CLUSTER.lock(CLIENT_1, BLOCKING_LOCK_REQUEST);

        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                assertThat(CLUSTER.lock(CLIENT_2, BLOCKING_LOCK_REQUEST)).isNotNull();
            } catch (InterruptedException e) {
                fail(String.format("The lock request for client %s was interrupted", CLIENT_2), e);
            }
        });

        Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
        TestableTimelockServer leader = CLUSTER.currentLeader();
        CLUSTER.nonLeaders().forEach(TestableTimelockServer::kill);

        // Lock on leader so that AwaitingLeadershipProxy notices leadership loss.
        assertThatThrownBy(() -> leader.lock(CLIENT_3, BLOCKING_LOCK_REQUEST)).isInstanceOf(RetryableException.class);

        CLUSTER.nonLeaders().forEach(TestableTimelockServer::start);

        // Wait for the client2 to actually get the lock.
        Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
    }

    @Test
    public void nonLeadersReturn503() {
        CLUSTER.nonLeaders().forEach(server -> {
            assertThatThrownBy(() -> server.getFreshTimestamp())
                    .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);
            assertThatThrownBy(() -> server.lock(LOCK_CLIENT, LOCK_REQUEST))
                    .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);
        });
    }

    @Test
    public void leaderRespondsToRequests() throws InterruptedException {
        CLUSTER.currentLeader().getFreshTimestamp();

        LockRefreshToken token = CLUSTER.currentLeader().lock(LOCK_CLIENT, LOCK_REQUEST);
        CLUSTER.unlock(token);
    }

    @Test
    public void newLeaderTakesOverIfCurrentLeaderDies() {
        CLUSTER.currentLeader().kill();

        CLUSTER.getFreshTimestamp();
    }

    @Test
    public void leaderLosesLeadersipIfQuorumIsNotAlive() {
        TestableTimelockServer leader = CLUSTER.currentLeader();
        CLUSTER.nonLeaders().forEach(TestableTimelockServer::kill);

        assertThatThrownBy(() -> leader.getFreshTimestamp())
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
    public void locksAreInvalidatedAcrossFailures() throws InterruptedException {
        LockRefreshToken token = CLUSTER.lock(LOCK_CLIENT, LOCK_REQUEST);

        for (int i = 0; i < 3; i++) {
            CLUSTER.failoverToNewLeader();

            assertThat(CLUSTER.unlock(token)).isFalse();
            token = CLUSTER.lock(LOCK_CLIENT, LOCK_REQUEST);
        }
    }
}
