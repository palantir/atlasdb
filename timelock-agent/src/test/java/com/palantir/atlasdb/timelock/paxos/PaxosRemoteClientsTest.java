/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.paxos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.concurrent.CheckedRejectionExecutorService;
import com.palantir.conjure.java.api.config.service.PartialServiceConfiguration;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.sls.versions.OrderableSlsVersion;
import com.palantir.timelock.config.ImmutableDefaultClusterConfiguration;
import com.palantir.timelock.config.ImmutablePaxosTsBoundPersisterConfiguration;
import com.palantir.timelock.config.PaxosInstallConfiguration;
import com.palantir.timelock.config.SqlitePaxosPersistenceConfigurations;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.paxos.TimeLockDialogueServiceProvider;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PaxosRemoteClientsTest {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private PaxosInstallConfiguration paxosInstallConfiguration;
    private TimeLockInstallConfiguration installConfiguration;
    private PaxosResourcesFactory.TimelockPaxosInstallationContext context;

    @Before
    public void setUp() {
        paxosInstallConfiguration = PaxosInstallConfiguration.builder()
                .isNewService(false)
                .dataDirectory(temporaryFolder.getRoot())
                .sqlitePersistence(SqlitePaxosPersistenceConfigurations.DEFAULT)
                .leaderMode(PaxosInstallConfiguration.PaxosLeaderMode.SINGLE_LEADER)
                .build();
        installConfiguration = TimeLockInstallConfiguration.builder()
                .cluster(ImmutableDefaultClusterConfiguration.builder()
                        .localServer("a:1")
                        .cluster(
                                PartialServiceConfiguration.of(ImmutableList.of("a:1", "b:2", "c:3"), Optional.empty()))
                        .build())
                .paxos(paxosInstallConfiguration) // Normally awful, but too onerous to set up
                .timestampBoundPersistence(
                        ImmutablePaxosTsBoundPersisterConfiguration.builder().build())
                .build();
        TimeLockDialogueServiceProvider dialogueServiceProvider = mock(TimeLockDialogueServiceProvider.class);
        when(dialogueServiceProvider.createSingleNodeInstrumentedProxy(anyString(), any(), anyBoolean()))
                .thenAnswer(invocation -> mock(invocation.getArgument(1)));
        context = ImmutableTimelockPaxosInstallationContext.builder()
                .install(installConfiguration)
                .dialogueServiceProvider(dialogueServiceProvider)
                .userAgent(UserAgent.of(UserAgent.Agent.of("aaa", "1.2.3")))
                .timeLockVersion(OrderableSlsVersion.valueOf("0.0.0"))
                .build();
    }

    @Test
    public void pingAndPaxosHaveDistinctExecutors() {
        PaxosRemoteClients remoteClients = ImmutablePaxosRemoteClients.builder()
                .context(context)
                .metrics(MetricsManagers.createForTests())
                .build();
        Set<CheckedRejectionExecutorService> leaderPingExecutors =
                remoteClients.batchPingableLeadersWithContext().stream()
                        .map(WithDedicatedExecutor::executor)
                        .collect(Collectors.toSet());
        Set<CheckedRejectionExecutorService> paxosExecutionExecutors =
                remoteClients.nonBatchTimestampAcceptor().stream()
                        .map(WithDedicatedExecutor::executor)
                        .collect(Collectors.toSet());
        assertThat(leaderPingExecutors).doesNotContainAnyElementsOf(paxosExecutionExecutors);
    }
}
