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

package com.palantir.timelock.history;

import static org.assertj.core.api.Assertions.assertThat;

import static com.palantir.timelock.history.utils.PaxosSerializationTestUtils.createAndWriteValueForLogAndRound;
import static com.palantir.timelock.history.utils.PaxosSerializationTestUtils.writeAcceptorStateForLogAndRound;

import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.palantir.paxos.Client;
import com.palantir.paxos.ImmutableNamespaceAndUseCase;
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.paxos.PaxosAcceptorState;
import com.palantir.paxos.PaxosStateLog;
import com.palantir.paxos.PaxosValue;
import com.palantir.paxos.SqliteConnections;
import com.palantir.paxos.SqlitePaxosStateLog;
import com.palantir.timelock.history.models.LearnerAndAcceptorRecords;
import com.palantir.timelock.history.sqlite.SqlitePaxosStateLogHistory;
import com.palantir.timelock.history.util.UseCaseUtils;
import com.palantir.timelock.history.utils.PaxosSerializationTestUtils;


public class SqliteHistoryQueriesTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final Client CLIENT = Client.of("tom");
    private static final String USE_CASE_LEARNER = "useCase!learner";
    private static final String USE_CASE_ACCEPTOR = "useCase!acceptor";

    private DataSource dataSource;
    private PaxosStateLog<PaxosValue> learnerLog;
    private PaxosStateLog<PaxosAcceptorState> acceptorLog;
    private LocalHistoryLoader history;

    @Before
    public void setup() {
        dataSource = SqliteConnections.getPooledDataSource(tempFolder.getRoot().toPath());
        learnerLog = SqlitePaxosStateLog.create(ImmutableNamespaceAndUseCase.of(CLIENT, USE_CASE_LEARNER), dataSource);
        acceptorLog =
                SqlitePaxosStateLog.create(ImmutableNamespaceAndUseCase.of(CLIENT, USE_CASE_ACCEPTOR), dataSource);
        history = LocalHistoryLoader.create(SqlitePaxosStateLogHistory.create(dataSource));
    }

    @Test
    public void canGetAllLearnerLogsSince() {
        IntStream.rangeClosed(1, 100).forEach(i -> createAndWriteValueForLogAndRound(learnerLog, i));
        LearnerAndAcceptorRecords learnerAndAcceptorRecords = history.loadLocalHistory(
                ImmutableNamespaceAndUseCase.of(CLIENT, UseCaseUtils.getPaxosUseCasePrefix(USE_CASE_LEARNER)), 5L);
        assertThat(learnerAndAcceptorRecords.acceptorRecords().size()).isEqualTo(0);
        assertThat(learnerAndAcceptorRecords.learnerRecords().size()).isEqualTo(95);
    }

    @Test
    public void canGetAllCorrectLearnerLogsSince() {
        long round = 1;
        createAndWriteValueForLogAndRound(learnerLog, round);
        LearnerAndAcceptorRecords learnerAndAcceptorRecords = history.loadLocalHistory(
                ImmutableNamespaceAndUseCase.of(CLIENT, UseCaseUtils.getPaxosUseCasePrefix(USE_CASE_LEARNER)), 0L);
        assertThat(learnerAndAcceptorRecords.acceptorRecords().size()).isEqualTo(0);
        assertThat(learnerAndAcceptorRecords.learnerRecords().size()).isEqualTo(1);

        PaxosValue paxosValue = learnerAndAcceptorRecords.learnerRecords().get(round);
        assertThat(paxosValue.getRound()).isEqualTo(round);
        assertThat(paxosValue.getData()).isEqualTo(PaxosSerializationTestUtils.longToBytes(round));
    }

    @Test
    public void canGetAllAcceptorLogsSince() {
        IntStream.rangeClosed(1, 100).forEach(i -> writeAcceptorStateForLogAndRound(acceptorLog, i, Optional.empty()));
        LearnerAndAcceptorRecords learnerAndAcceptorRecords = history.loadLocalHistory(
                ImmutableNamespaceAndUseCase.of(CLIENT, UseCaseUtils.getPaxosUseCasePrefix(USE_CASE_LEARNER)), 5L);
        assertThat(learnerAndAcceptorRecords.learnerRecords().size()).isEqualTo(0);
        assertThat(learnerAndAcceptorRecords.acceptorRecords().size()).isEqualTo(95);
    }

    @Test
    public void canGetAllLearnerAndAcceptorLogsSince() {
        PaxosSerializationTestUtils.writeToLogs(acceptorLog, learnerLog, 1, 100);
        LearnerAndAcceptorRecords learnerAndAcceptorRecords = history.loadLocalHistory(
                ImmutableNamespaceAndUseCase.of(CLIENT, UseCaseUtils.getPaxosUseCasePrefix(USE_CASE_LEARNER)), 5L);
        assertThat(learnerAndAcceptorRecords.learnerRecords().size()).isEqualTo(95);
        assertThat(learnerAndAcceptorRecords.acceptorRecords().size()).isEqualTo(95);
    }

    @Test
    public void canGetAllLearnerAndAcceptorDiscontinuousLogsSince() {
        int startInclusive = 55;
        PaxosSerializationTestUtils.writeToLogs(acceptorLog, learnerLog, startInclusive, 100);

        LearnerAndAcceptorRecords learnerAndAcceptorRecords = history.loadLocalHistory(
                ImmutableNamespaceAndUseCase.of(CLIENT, UseCaseUtils.getPaxosUseCasePrefix(USE_CASE_LEARNER)), 5L);

        int expected = 100 - startInclusive + 1;
        assertThat(learnerAndAcceptorRecords.learnerRecords().size()).isEqualTo(expected);
        assertThat(learnerAndAcceptorRecords.acceptorRecords().size()).isEqualTo(expected);
    }

    @Test
    public void canGetAllUniquePairsOfNamespaceAndClient() {
        IntStream.range(0, 100).forEach(i -> {
            PaxosStateLog<PaxosValue> otherLog
                    = SqlitePaxosStateLog.create(ImmutableNamespaceAndUseCase.of(Client.of("client" + i),
                    USE_CASE_LEARNER), dataSource);
            createAndWriteValueForLogAndRound(otherLog, 1L);
        });
        Set<NamespaceAndUseCase> allNamespaceAndUseCaseTuples =
                SqlitePaxosStateLogHistory.create(dataSource).getAllNamespaceAndUseCaseTuples();
        assertThat(allNamespaceAndUseCaseTuples.size()).isEqualTo(100);
    }
}
