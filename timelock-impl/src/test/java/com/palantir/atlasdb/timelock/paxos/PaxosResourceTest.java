/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock.paxos;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosProposal;
import com.palantir.paxos.PaxosProposalId;
import com.palantir.paxos.PaxosValue;

public class PaxosResourceTest {
    private static final String CLIENT_1 = "alice";
    private static final String CLIENT_2 = "bob";

    private static final long PAXOS_ROUND_ONE = 1;
    private static final long PAXOS_ROUND_TWO = 2;
    private static final String PAXOS_UUID = "paxos";
    private static final byte[] PAXOS_DATA = { 0 };
    private static final PaxosValue PAXOS_VALUE = new PaxosValue(PAXOS_UUID, PAXOS_ROUND_ONE, PAXOS_DATA);
    private static final PaxosProposal PAXOS_PROPOSAL = new PaxosProposal(
            new PaxosProposalId(PAXOS_ROUND_TWO, PAXOS_UUID), PAXOS_VALUE);

    @ClassRule
    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private PaxosResource paxosResource;
    private File logDirectory;

    @Before
    public void setUp() throws IOException {
        logDirectory = TEMPORARY_FOLDER.newFolder();
        paxosResource = PaxosResource.create(logDirectory.getPath());
    }

    @Test
    public void newClientCanBeCreated() {
        PaxosLearner learner = paxosResource.getPaxosLearner(CLIENT_1);
        learner.learn(PAXOS_ROUND_ONE, PAXOS_VALUE);
        assertThat(learner.getGreatestLearnedValue()).isNotNull();
        assertThat(learner.getGreatestLearnedValue().getLeaderUUID()).isEqualTo(PAXOS_UUID);
        assertThat(learner.getGreatestLearnedValue().getData()).isEqualTo(PAXOS_DATA);

        PaxosAcceptor acceptor = paxosResource.getPaxosAcceptor(CLIENT_1);
        acceptor.accept(PAXOS_ROUND_TWO, PAXOS_PROPOSAL);
        assertThat(acceptor.getLatestSequencePreparedOrAccepted()).isEqualTo(PAXOS_ROUND_TWO);
    }

    @Test
    public void addsClientsInSubdirectory() {
        paxosResource.getPaxosLearner(CLIENT_1);
        File expectedAcceptorLogDir =
                Paths.get(logDirectory.getPath(), CLIENT_1, PaxosTimeLockConstants.ACCEPTOR_SUBDIRECTORY_PATH).toFile();
        assertThat(expectedAcceptorLogDir.exists()).isTrue();
        File expectedLearnerLogDir =
                Paths.get(logDirectory.getPath(), CLIENT_1, PaxosTimeLockConstants.LEARNER_SUBDIRECTORY_PATH).toFile();
        assertThat(expectedLearnerLogDir.exists()).isTrue();
    }

}
