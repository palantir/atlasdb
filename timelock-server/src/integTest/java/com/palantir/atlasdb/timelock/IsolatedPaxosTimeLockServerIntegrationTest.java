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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants;
import com.palantir.atlasdb.timelock.util.ExceptionMatchers;
import com.palantir.atlasdb.timelock.util.TestProxies;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;

/**
 * This test creates a single TimeLock server that is configured in a three node configuration.
 * Since it has no quorum, timestamp and remoteLock requests (and fast-forward) should fail.
 * However it should still be pingable and should be able to participate in Paxos as well.
 */
public class IsolatedPaxosTimeLockServerIntegrationTest {
    private static final String CLIENT = "isolated";

    private static final TestableTimelockCluster CLUSTER = new TestableTimelockCluster(
            "https://localhost",
            CLIENT,
            "paxosThreeServers.yml");

    private static final TestableTimelockServer SERVER = CLUSTER.servers().get(0);

    @ClassRule
    public static final RuleChain ruleChain = CLUSTER.getRuleChain();

    @Test
    public void cannotIssueTimestampsWithoutQuorum() {
        assertThatThrownBy(() -> SERVER.getFreshTimestamp())
                .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);
    }

    @Test
    public void cannotIssueLocksWithoutQuorum() {
        assertThatThrownBy(() -> SERVER.lockService().currentTimeMillis())
                .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);
    }

    @Test
    public void cannotPerformTimestampManagementWithoutQuorum() {
        assertThatThrownBy(() -> SERVER.timestampManagementService().fastForwardTimestamp(1000L))
                .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);
    }

    @Test
    public void canPingWithoutQuorum() {
        SERVER.leaderPing(); // should succeed
    }

    @Test
    public void canParticipateInPaxosAsAcceptorWithoutQuorum() {
        PaxosAcceptor acceptor = createProxyForInternalNamespacedTestService(PaxosAcceptor.class);
        acceptor.getLatestSequencePreparedOrAccepted();
    }

    @Test
    public void canParticipateInPaxosAsLearnerWithoutQuorum() {
        PaxosLearner learner = createProxyForInternalNamespacedTestService(PaxosLearner.class);
        learner.getGreatestLearnedValue();
    }

    private static <T> T createProxyForInternalNamespacedTestService(Class<T> clazz) {
        return AtlasDbHttpClients.createProxy(
                Optional.of(TestProxies.SSL_SOCKET_FACTORY),
                Optional::empty,
                String.format("https://localhost:%d/%s/%s/%s",
                        SERVER.serverHolder().getTimelockPort(),
                        PaxosTimeLockConstants.INTERNAL_NAMESPACE,
                        PaxosTimeLockConstants.CLIENT_PAXOS_NAMESPACE,
                        CLIENT),
                clazz);
    }

}
