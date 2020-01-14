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

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.suite.PaxosSuite;
import com.palantir.atlasdb.timelock.util.ExceptionMatchers;
import com.palantir.atlasdb.timelock.util.ParameterInjector;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockToken;

@RunWith(Parameterized.class)
public class MultiNodePaxosTimeLockServerIntegrationTest {

    @ClassRule
    public static ParameterInjector<TestableTimelockCluster> injector =
            ParameterInjector.withFallBackConfiguration(() -> PaxosSuite.BATCHED_TIMESTAMP_PAXOS);

    @Parameterized.Parameter
    public TestableTimelockCluster cluster;

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<TestableTimelockCluster> params() {
        return injector.getParameter();
    }

    private static final LockDescriptor LOCK = StringLockDescriptor.of("foo");
    private static final Set<LockDescriptor> LOCKS = ImmutableSet.of(LOCK);

    private static final int DEFAULT_LOCK_TIMEOUT_MS = 10_000;

    private NamespacedClients client;

    @Before
    public void bringAllNodesOnline() {
        client = cluster.clientForRandomNamespace();
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
    public void leaderRespondsToRequests() {
        NamespacedClients currentLeader = cluster.currentLeaderFor(client.namespace())
                .client(client.namespace());
        currentLeader.getFreshTimestamp();

        LockToken token = currentLeader.lock(LockRequest.of(LOCKS, DEFAULT_LOCK_TIMEOUT_MS)).getToken();
        currentLeader.unlock(token);
    }

    @Test
    public void newLeaderTakesOverIfCurrentLeaderDies() {
        cluster.currentLeaderFor(client.namespace()).kill();

        assertThatCode(client::getFreshTimestamp)
                .doesNotThrowAnyException();
    }

    @Test
    public void leaderLosesLeadershipIfQuorumIsNotAlive() {
        NamespacedClients leader = cluster.currentLeaderFor(client.namespace())
                .client(client.namespace());
        cluster.nonLeaders(client.namespace()).forEach((unused, server) -> server.kill());

        assertThatThrownBy(leader::getFreshTimestamp)
                .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);
    }

    @Test
    public void someoneBecomesLeaderAgainAfterQuorumIsRestored() {
        cluster.nonLeaders(client.namespace()).forEach((unused, server) -> server.kill());
        cluster.nonLeaders(client.namespace()).forEach((unused, server) -> server.start());

        client.getFreshTimestamp();
    }

    @Test
    public void canPerformRollingRestart() {
        bringAllNodesOnline();
        for (TestableTimelockServer server : cluster.servers()) {
            server.kill();
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
        leaderTimes.add(client.namespacedTimelockRpcClient().getLeaderTime());

        for (int i = 0; i < 3; i++) {
            cluster.failoverToNewLeader(client.namespace());

            LeaderTime leaderTime = client.namespacedTimelockRpcClient().getLeaderTime();

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
            NamespacedClients randomNamespace = cluster.clientForRandomNamespace();

            randomNamespace.getFreshTimestamp();
            LockToken token = randomNamespace.lock(LockRequest.of(LOCKS, DEFAULT_LOCK_TIMEOUT_MS)).getToken();
            randomNamespace.unlock(token);
        }
    }

}
