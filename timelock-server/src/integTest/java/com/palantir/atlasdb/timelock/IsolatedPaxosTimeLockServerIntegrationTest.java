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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.http.TestProxyUtils;
import com.palantir.atlasdb.timelock.ImmutableTemplateVariables.TimestampPaxos;
import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants;
import com.palantir.atlasdb.timelock.util.ExceptionMatchers;
import com.palantir.atlasdb.timelock.util.TestProxies;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.timelock.config.PaxosInstallConfiguration.PaxosLeaderMode;
import java.util.Optional;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

/**
 * This test creates a single TimeLock server that is configured in a three node configuration.
 * Since it has no quorum, timestamp and remoteLock requests (and fast-forward) should fail.
 * However it should still be pingable and should be able to participate in Paxos as well.
 */
public class IsolatedPaxosTimeLockServerIntegrationTest {

    private static final TemplateVariables SINGLE_NODE = ImmutableTemplateVariables.builder()
            .localServerPort(9060)
            .clientPaxos(TimestampPaxos.builder().isUseBatchPaxosTimestamp(false).build())
            .leaderMode(PaxosLeaderMode.SINGLE_LEADER)
            .build();

    private static final TestableTimelockCluster CLUSTER = new TestableTimelockCluster(
            "paxosStaticThreeServers.ftl", SINGLE_NODE);

    private static final TestableTimelockServer SERVER = Iterables.getOnlyElement(CLUSTER.servers());

    @ClassRule
    public static final RuleChain ruleChain = CLUSTER.getRuleChain();

    private NamespacedClients namespace;

    @Before
    public void setUp() {
        namespace = CLUSTER.clientForRandomNamespace();
    }

    @Test
    public void cannotIssueTimestampsWithoutQuorum() {
        assertThatThrownBy(() -> CLUSTER.clientForRandomNamespace().getFreshTimestamp())
                .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);
    }

    @Test
    public void cannotIssueLocksWithoutQuorum() {
        assertThatThrownBy(() -> CLUSTER.clientForRandomNamespace().timelockService().currentTimeMillis())
                .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);
    }

    @Test
    public void cannotPerformTimestampManagementWithoutQuorum() {
        assertThatThrownBy(
                () -> CLUSTER.clientForRandomNamespace().timestampManagementService().fastForwardTimestamp(1000L))
                .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);
    }

    @Test
    public void canPingWithoutQuorum() {
        assertThatCode(() -> SERVER.pinger().ping(namespace.namespace()))
                .doesNotThrowAnyException();
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

    private <T> T createProxyForInternalNamespacedTestService(Class<T> clazz) {
        return AtlasDbHttpClients.createProxy(
                Optional.of(TestProxies.TRUST_CONTEXT),
                String.format("https://localhost:%d/%s/%s/%s",
                        SERVER.serverHolder().getTimelockPort(),
                        PaxosTimeLockConstants.INTERNAL_NAMESPACE,
                        PaxosTimeLockConstants.CLIENT_PAXOS_NAMESPACE,
                        namespace.namespace()),
                clazz,
                TestProxyUtils.AUXILIARY_REMOTING_PARAMETERS_RETRYING);
    }

}
