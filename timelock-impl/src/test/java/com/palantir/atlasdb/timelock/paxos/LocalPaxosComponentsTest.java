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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosProposal;
import com.palantir.paxos.PaxosProposalId;
import com.palantir.paxos.PaxosValue;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;

public class LocalPaxosComponentsTest {
    private static final Client CLIENT = Client.of("alice");

    private static final long PAXOS_ROUND_ONE = 1;
    private static final long PAXOS_ROUND_TWO = 2;
    private static final String PAXOS_UUID = "paxos";
    private static final byte[] PAXOS_DATA = { 0 };
    private static final PaxosValue PAXOS_VALUE = new PaxosValue(PAXOS_UUID, PAXOS_ROUND_ONE, PAXOS_DATA);
    private static final PaxosProposal PAXOS_PROPOSAL = new PaxosProposal(
            new PaxosProposalId(PAXOS_ROUND_TWO, PAXOS_UUID), PAXOS_VALUE);

    @Rule
    public final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private LocalPaxosComponents paxosComponents;
    private Path logDirectory;

    @Before
    public void setUp() throws IOException {
        logDirectory = TEMPORARY_FOLDER.newFolder().toPath();
        paxosComponents = new LocalPaxosComponents(
                TimelockPaxosMetrics.of(PaxosUseCase.TIMESTAMP, new DefaultTaggedMetricRegistry()),
                logDirectory,
                UUID.randomUUID());
    }

    @Test
    public void newClientCanBeCreated() {
        PaxosLearner learner = paxosComponents.learner(CLIENT);
        learner.learn(PAXOS_ROUND_ONE, PAXOS_VALUE);

        assertThat(learner.getGreatestLearnedValue())
                .isPresent()
                .hasValueSatisfying(paxosValue -> {
                    assertThat(paxosValue.getLeaderUUID()).isEqualTo(PAXOS_UUID);
                    assertThat(paxosValue.getData()).isEqualTo(PAXOS_DATA);
                });

        PaxosAcceptor acceptor = paxosComponents.acceptor(CLIENT);
        acceptor.accept(PAXOS_ROUND_TWO, PAXOS_PROPOSAL);
        assertThat(acceptor.getLatestSequencePreparedOrAccepted()).isEqualTo(PAXOS_ROUND_TWO);
    }

    @Test
    public void addsClientsInSubdirectory() {
        paxosComponents.learner(CLIENT);
        File expectedAcceptorLogDir = logDirectory.resolve(
                Paths.get(CLIENT.value(), PaxosTimeLockConstants.ACCEPTOR_SUBDIRECTORY_PATH)).toFile();
        assertThat(expectedAcceptorLogDir.exists()).isTrue();
        File expectedLearnerLogDir = logDirectory.resolve(
                Paths.get(CLIENT.value(), PaxosTimeLockConstants.LEARNER_SUBDIRECTORY_PATH)).toFile();
        assertThat(expectedLearnerLogDir.exists()).isTrue();
    }

}
