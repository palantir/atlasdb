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

import java.io.File;

import javax.net.ssl.SSLSocketFactory;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import com.google.common.base.Optional;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants;
import com.palantir.leader.PingableLeader;
import com.palantir.lock.RemoteLockService;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;

import feign.RetryableException;
import io.dropwizard.testing.ResourceHelpers;

/**
 * This test creates a single TimeLock server that is configured in a three node configuration.
 * Since it has no quorum, timestamp and lock requests (and fast-forward) should fail.
 * However it should still be pingable and should be able to participate in Paxos as well.
 */
public class IsolatedPaxosTimeLockServerIntegrationTest {
    private static final String CLIENT = "isolated";

    private static final Optional<SSLSocketFactory> NO_SSL = Optional.absent();

    private static final File TIMELOCK_CONFIG_TEMPLATE =
            new File(ResourceHelpers.resourceFilePath("paxosThreeServers.yml"));

    private static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
    private static final TemporaryConfigurationHolder TEMPORARY_CONFIG_HOLDER =
            new TemporaryConfigurationHolder(TEMPORARY_FOLDER, TIMELOCK_CONFIG_TEMPLATE);
    private static final TimeLockServerHolder TIMELOCK_SERVER_HOLDER =
            new TimeLockServerHolder(TEMPORARY_CONFIG_HOLDER::getTemporaryConfigFileLocation);

    @ClassRule
    public static final RuleChain ruleChain = RuleChain.outerRule(TEMPORARY_FOLDER)
            .around(TEMPORARY_CONFIG_HOLDER)
            .around(TIMELOCK_SERVER_HOLDER);

    @Test
    public void cannotIssueTimestampsWithoutQuorum() {
        assertThatThrownBy(() -> getTimestampService(CLIENT).getFreshTimestamp())
                .satisfies(IsolatedPaxosTimeLockServerIntegrationTest::isRetryableExceptionWhereLeaderCannotBeFound);
    }

    @Test
    public void cannotIssueLocksWithoutQuorum() {
        assertThatThrownBy(() -> getLockService(CLIENT).currentTimeMillis())
                .satisfies(IsolatedPaxosTimeLockServerIntegrationTest::isRetryableExceptionWhereLeaderCannotBeFound);
    }

    @Test
    public void cannotPerformTimestampManagementWithoutQuorum() {
        assertThatThrownBy(() -> getTimestampManagementService(CLIENT).fastForwardTimestamp(1000L))
                .satisfies(IsolatedPaxosTimeLockServerIntegrationTest::isRetryableExceptionWhereLeaderCannotBeFound);
    }

    @Test
    public void canPingWithoutQuorum() {
        PingableLeader leader = AtlasDbHttpClients.createProxy(
                NO_SSL,
                "http://localhost:" + TIMELOCK_SERVER_HOLDER.getTimelockPort(),
                PingableLeader.class);
        leader.ping(); // should succeed
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

    private static void isRetryableExceptionWhereLeaderCannotBeFound(Throwable throwable) {
        assertThat(throwable).isInstanceOf(RetryableException.class)
                .hasMessageContaining("method invoked on a non-leader");
    }

    private static RemoteLockService getLockService(String client) {
        return getProxyForService(client, RemoteLockService.class);
    }

    private static TimestampService getTimestampService(String client) {
        return getProxyForService(client, TimestampService.class);
    }

    private static TimestampManagementService getTimestampManagementService(String client) {
        return getProxyForService(client, TimestampManagementService.class);
    }

    private static <T> T getProxyForService(String client, Class<T> clazz) {
        return AtlasDbHttpClients.createProxy(
                NO_SSL,
                getRootUriForClient(client),
                clazz);
    }

    private static <T> T createProxyForInternalNamespacedTestService(Class<T> clazz) {
        return AtlasDbHttpClients.createProxy(
                NO_SSL,
                String.format("http://localhost:%d/%s/%s/%s",
                        TIMELOCK_SERVER_HOLDER.getTimelockPort(),
                        PaxosTimeLockConstants.INTERNAL_NAMESPACE,
                        PaxosTimeLockConstants.CLIENT_PAXOS_NAMESPACE,
                        CLIENT),
                clazz);
    }

    private static String getRootUriForClient(String client) {
        return String.format("http://localhost:%d/%s", TIMELOCK_SERVER_HOLDER.getTimelockPort(), client);
    }
}
