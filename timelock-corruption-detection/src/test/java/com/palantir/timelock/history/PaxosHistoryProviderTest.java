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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableList;
import com.palantir.paxos.Client;
import com.palantir.paxos.ImmutableNamespaceAndUseCase;
import com.palantir.paxos.PaxosAcceptorState;
import com.palantir.paxos.PaxosStateLog;
import com.palantir.paxos.PaxosValue;
import com.palantir.paxos.SqliteConnections;
import com.palantir.paxos.SqlitePaxosStateLog;
import com.palantir.timelock.history.models.CompletePaxosHistoryForNamespaceAndUseCase;
import com.palantir.timelock.history.models.ConsolidatedLearnerAndAcceptorRecord;
import com.palantir.timelock.history.remote.HistoryLoaderAndTransformer;
import com.palantir.timelock.history.sqlite.LogVerificationProgressState;
import com.palantir.timelock.history.sqlite.SqlitePaxosStateLogHistory;
import com.palantir.timelock.history.utils.PaxosSerializationTestUtils;

public class PaxosHistoryProviderTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final Client CLIENT = Client.of("client");
    private static final String USE_CASE = "useCase";
    private static final String USE_CASE_LEARNER = "useCase!learner";
    private static final String USE_CASE_ACCEPTOR = "useCase!acceptor";
    private TimeLockPaxosHistoryProvider remote;

    private DataSource dataSource;
    private PaxosStateLog<PaxosValue> learnerLog;
    private PaxosStateLog<PaxosAcceptorState> acceptorLog;
    private LocalHistoryLoader history;
    private PaxosLogHistoryProvider paxosLogHistoryProvider;
    private LogVerificationProgressState verificationProgressState;

    @Before
    public void setup() {
        remote = mock(TimeLockPaxosHistoryProvider.class);
        dataSource = SqliteConnections.getPooledDataSource(tempFolder.getRoot().toPath());

        learnerLog = getLearnerLog(ImmutableNamespaceAndUseCase.of(CLIENT, USE_CASE_LEARNER));
        acceptorLog = getAcceptorLog(ImmutableNamespaceAndUseCase.of(CLIENT, USE_CASE_ACCEPTOR));

        history = LocalHistoryLoader.create(SqlitePaxosStateLogHistory.create(dataSource));

        verificationProgressState = LogVerificationProgressState.create(dataSource);
        paxosLogHistoryProvider = new PaxosLogHistoryProvider(dataSource, ImmutableList.of(remote));
    }

    @Test
    public void canFetchAndCombineHistoriesForLocalAndRemote() {
        PaxosSerializationTestUtils.writeToLogs(acceptorLog, learnerLog, 1, 100);
        int lastVerified = -1;

        List<HistoryQuery> historyQueries = ImmutableList.of(HistoryQuery.of(
                ImmutableNamespaceAndUseCase.of(CLIENT, USE_CASE), lastVerified));

        List<LogsForNamespaceAndUseCase> remoteHistory
                = HistoryLoaderAndTransformer.getLogsForHistoryQueries(history, historyQueries);

        when(remote.getPaxosHistory(any(), any())).thenReturn(PaxosHistoryOnRemote.of(remoteHistory));

        List<CompletePaxosHistoryForNamespaceAndUseCase> completeHistory = paxosLogHistoryProvider.getHistory();

        assertThat(completeHistory.size()).isEqualTo(1);
        CompletePaxosHistoryForNamespaceAndUseCase historyForNamespaceAndUseCase = completeHistory.get(0);
        assertThat(historyForNamespaceAndUseCase.namespace()).isEqualTo(CLIENT);
        assertSanityOfFetchedRecords(historyForNamespaceAndUseCase, 100);
    }

    @Test
    public void throwsIfRemoteThrows() {
        PaxosSerializationTestUtils.writeToLogs(acceptorLog, learnerLog, 1, 100);
        when(remote.getPaxosHistory(any(), any())).thenThrow(new RuntimeException());
        assertThatThrownBy(paxosLogHistoryProvider::getHistory)
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    public void canFetchAndCombineHistoriesSinceLastVerifiedState() {
        PaxosSerializationTestUtils.writeToLogs(acceptorLog, learnerLog, 1, 100);
        int lastVerified = 17;
        verificationProgressState.updateProgress(CLIENT, USE_CASE, lastVerified);

        List<HistoryQuery> historyQueries = ImmutableList.of(HistoryQuery.of(
                ImmutableNamespaceAndUseCase.of(CLIENT, USE_CASE), lastVerified));
        List<LogsForNamespaceAndUseCase> remoteHistory
                = HistoryLoaderAndTransformer.getLogsForHistoryQueries(history, historyQueries);

        when(remote.getPaxosHistory(any(), any())).thenReturn(PaxosHistoryOnRemote.of(remoteHistory));

        List<CompletePaxosHistoryForNamespaceAndUseCase> completeHistory = paxosLogHistoryProvider.getHistory();
        assertThat(completeHistory.size()).isEqualTo(1);

        CompletePaxosHistoryForNamespaceAndUseCase historyForNamespaceAndUseCase = completeHistory.get(0);
        assertThat(historyForNamespaceAndUseCase.namespace()).isEqualTo(CLIENT);
        assertSanityOfFetchedRecords(historyForNamespaceAndUseCase, 100 - lastVerified);
    }

    @Test
    public void canFetchAndCombineHistoriesAcrossNamespaceAndUseCases() {
        List<HistoryQuery> historyQueries = new ArrayList<>();
        IntStream.rangeClosed(1, 100).forEach(idx -> {
            Client client = Client.of("" + idx);
            PaxosSerializationTestUtils.writeToLogs(
                    getAcceptorLog(ImmutableNamespaceAndUseCase.of(client, USE_CASE_ACCEPTOR)),
                    getLearnerLog(ImmutableNamespaceAndUseCase.of(client, USE_CASE_LEARNER)),
                    1, idx);
            historyQueries.add(HistoryQuery.of(ImmutableNamespaceAndUseCase.of(client, USE_CASE), -1));
        });

        List<LogsForNamespaceAndUseCase> remoteHistory
                = HistoryLoaderAndTransformer.getLogsForHistoryQueries(history, historyQueries);
        when(remote.getPaxosHistory(any(), any())).thenReturn(PaxosHistoryOnRemote.of(remoteHistory));

        List<CompletePaxosHistoryForNamespaceAndUseCase> completeHistory = paxosLogHistoryProvider.getHistory();
        assertThat(completeHistory.size()).isEqualTo(100);
        completeHistory.forEach(historyForNamespaceAndUseCase -> assertSanityOfFetchedRecords(
                historyForNamespaceAndUseCase, getIntegerValueOfClient(historyForNamespaceAndUseCase)
        ));
    }

    // utils
    private PaxosStateLog<PaxosValue> getLearnerLog(ImmutableNamespaceAndUseCase namespaceAndUseCase) {
        return SqlitePaxosStateLog.create(namespaceAndUseCase, dataSource);
    }

    private PaxosStateLog<PaxosAcceptorState> getAcceptorLog(ImmutableNamespaceAndUseCase namespaceAndUseCase) {
        return SqlitePaxosStateLog.create(namespaceAndUseCase, dataSource);
    }

    private void assertSanityOfFetchedRecords(
            CompletePaxosHistoryForNamespaceAndUseCase historyForNamespaceAndUseCase,
            int numberOfLogs) {

        assertThat(historyForNamespaceAndUseCase.useCase()).isEqualTo(USE_CASE);

        List<ConsolidatedLearnerAndAcceptorRecord> localAndRemoteLearnerAndAcceptorRecords
                = historyForNamespaceAndUseCase.localAndRemoteLearnerAndAcceptorRecords();

        assertThat(localAndRemoteLearnerAndAcceptorRecords.size()).isEqualTo(2); // there is one local and one remote

        assertThat(localAndRemoteLearnerAndAcceptorRecords.get(0).record().size())
                .isEqualTo(localAndRemoteLearnerAndAcceptorRecords.get(1).record().size())
                .isEqualTo(numberOfLogs);
    }

    private int getIntegerValueOfClient(CompletePaxosHistoryForNamespaceAndUseCase historyForNamespaceAndUseCase) {
        return Integer.parseInt(historyForNamespaceAndUseCase.namespace().value());
    }

}
