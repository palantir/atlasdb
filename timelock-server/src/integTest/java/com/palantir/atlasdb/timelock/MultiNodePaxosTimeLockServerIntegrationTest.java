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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import com.google.common.collect.ImmutableSortedMap;
import com.palantir.atlasdb.timelock.util.ExceptionMatchers;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.StringLockDescriptor;

public class MultiNodePaxosTimeLockServerIntegrationTest {
    private static final String CLIENT_1 = "test";

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
    public void lockRequest500sIfLeaderElectionOccurs() throws InterruptedException {
        LockRequest request = LockRequest.builder(
                ImmutableSortedMap.of(
                        StringLockDescriptor.of("foo"),
                        LockMode.WRITE))
                .build();

        CLUSTER.lock("1", request);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            try {
                System.out.println("locking");
                CLUSTER.lock("2", request);
                System.out.println("locked");
            } catch (Throwable e) {
                System.out.print("failed to lock");
                e.printStackTrace();
            }
        });

        Thread.sleep(5000L);
        TestableTimelockServer leader = CLUSTER.currentLeader();
        CLUSTER.nonLeaders().forEach(server -> {
            server.kill();
        });
        try {
            leader.lock("3", request);
        } catch (Throwable t) {

        }
        LockSupport.park();
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
