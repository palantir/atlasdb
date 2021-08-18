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
package com.palantir.atlasdb.timelock.paxos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.leader.PingableLeader;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosProposal;
import com.palantir.paxos.PaxosProposalId;
import com.palantir.paxos.PaxosValue;
import com.palantir.paxos.SqliteConnections;
import com.palantir.sls.versions.OrderableSlsVersion;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LocalPaxosComponentsTest {
    private static final Client CLIENT = Client.of("alice");

    private static final long PAXOS_ROUND_ONE = 1;
    private static final long PAXOS_ROUND_TWO = 2;
    private static final String PAXOS_UUID = "paxos";
    private static final byte[] PAXOS_DATA = {0};
    private static final PaxosValue PAXOS_VALUE = new PaxosValue(PAXOS_UUID, PAXOS_ROUND_ONE, PAXOS_DATA);
    private static final PaxosProposal PAXOS_PROPOSAL =
            new PaxosProposal(new PaxosProposalId(PAXOS_ROUND_TWO, PAXOS_UUID), PAXOS_VALUE);
    private static final OrderableSlsVersion DEFAULT_TIME_LOCK_VERSION = OrderableSlsVersion.valueOf("0.0.0");
    private static final OrderableSlsVersion TIMELOCK_VERSION = OrderableSlsVersion.valueOf("1.2.7");

    @Rule
    public final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private LocalPaxosComponents paxosComponents;
    private Path legacyDirectory;
    private DataSource sqlite;

    @Before
    public void setUp() throws IOException {
        legacyDirectory = TEMPORARY_FOLDER.newFolder("legacy").toPath();
        sqlite = SqliteConnections.getDefaultConfiguredPooledDataSource(
                TEMPORARY_FOLDER.newFolder("sqlite").toPath());
        paxosComponents = createPaxosComponents(true);
    }

    @Test
    public void newClientCanBeCreated() {
        PaxosLearner learner = paxosComponents.learner(CLIENT);
        learner.learn(PAXOS_ROUND_ONE, PAXOS_VALUE);

        assertThat(learner.getGreatestLearnedValue())
                .map(PaxosValue::getLeaderUUID)
                .contains(PAXOS_UUID);
        assertThat(learner.getGreatestLearnedValue()).map(PaxosValue::getData).contains(PAXOS_DATA);

        PaxosAcceptor acceptor = paxosComponents.acceptor(CLIENT);
        acceptor.accept(PAXOS_ROUND_TWO, PAXOS_PROPOSAL);
        assertThat(acceptor.getLatestSequencePreparedOrAccepted()).isEqualTo(PAXOS_ROUND_TWO);
    }

    @Test
    public void addsClientsInSubdirectory() {
        paxosComponents.learner(CLIENT);
        File expectedAcceptorLogDir = legacyDirectory
                .resolve(Paths.get(CLIENT.value(), PaxosTimeLockConstants.ACCEPTOR_SUBDIRECTORY_PATH))
                .toFile();
        assertThat(expectedAcceptorLogDir.exists()).isTrue();
        File expectedLearnerLogDir = legacyDirectory
                .resolve(Paths.get(CLIENT.value(), PaxosTimeLockConstants.LEARNER_SUBDIRECTORY_PATH))
                .toFile();
        assertThat(expectedLearnerLogDir.exists()).isTrue();
    }

    @Test
    public void newClientCannotBeCreatedIfCreatingClientsIsNotPermitted() {
        LocalPaxosComponents rejectingComponents = createPaxosComponents(false);
        assertThatThrownBy(() -> rejectingComponents.learner(CLIENT))
                .isInstanceOf(ServiceNotAvailableException.class)
                .hasMessageContaining("not allowed to create new clients at this time")
                .hasMessageContaining(CLIENT.value());
    }

    @Test
    public void newClientCanBeCreatedIfItAlreadyExistsInTheDirectory() {
        paxosComponents.learner(CLIENT);
        LocalPaxosComponents rejectingComponents = createPaxosComponents(false);
        assertThat(rejectingComponents.learner(CLIENT)).isNotNull();
    }

    @Test
    public void returnsTimeLockVersionWithIsLeaderBoolean() {
        // return default when timeLock version not provided
        PingableLeader pingableLeader = paxosComponents.pingableLeader(CLIENT);
        assertThat(pingableLeader.pingV2().timeLockVersion()).isEqualTo(Optional.of(DEFAULT_TIME_LOCK_VERSION));
        assertThat(pingableLeader.pingV2().isLeader()).isNotNull();

        pingableLeader = createPaxosComponents(true, TIMELOCK_VERSION).pingableLeader(CLIENT);
        assertThat(pingableLeader.pingV2().timeLockVersion()).hasValue(TIMELOCK_VERSION);
    }

    // utils
    public LocalPaxosComponents createPaxosComponents(boolean canCreateNewClients) {
        return LocalPaxosComponents.createWithAsyncMigration(
                TimelockPaxosMetrics.of(PaxosUseCase.TIMESTAMP, MetricsManagers.createForTests()),
                PaxosUseCase.TIMESTAMP,
                legacyDirectory,
                sqlite,
                UUID.randomUUID(),
                canCreateNewClients,
                DEFAULT_TIME_LOCK_VERSION,
                false);
    }

    public LocalPaxosComponents createPaxosComponents(
            boolean canCreateNewClients, OrderableSlsVersion timeLockVersion) {
        return LocalPaxosComponents.createWithAsyncMigration(
                TimelockPaxosMetrics.of(PaxosUseCase.TIMESTAMP, MetricsManagers.createForTests()),
                PaxosUseCase.TIMESTAMP,
                legacyDirectory,
                sqlite,
                UUID.randomUUID(),
                canCreateNewClients,
                timeLockVersion,
                false);
    }
}
