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

package com.palantir.history;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static com.palantir.history.utils.Utils.writeToLogs;

import java.util.List;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.history.models.CompletePaxosHistoryForNamespaceAndUseCase;
import com.palantir.history.sqlite.LogVerificationProgressState;
import com.palantir.history.sqlite.SqlitePaxosStateLogHistory;
import com.palantir.paxos.Client;
import com.palantir.paxos.ImmutableNamespaceAndUseCase;
import com.palantir.paxos.PaxosAcceptorState;
import com.palantir.paxos.PaxosStateLog;
import com.palantir.paxos.PaxosValue;
import com.palantir.paxos.SqliteConnections;
import com.palantir.paxos.SqlitePaxosStateLog;
import com.palantir.timelock.history.HistoryQuery;
import com.palantir.timelock.history.LogsForNamespaceAndUseCase;
import com.palantir.timelock.history.TimeLockPaxosHistoryProvider;
import com.palantir.tokens.auth.AuthHeader;

public class PaxosHistoryProviderTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final Client CLIENT = Client.of("client");
    private static final String USE_CASE = "useCase";
    private static final String USE_CASE_LEARNER = "useCase!learner";
    private static final String USE_CASE_ACCEPTOR = "useCase!acceptor";
    private static final TimeLockPaxosHistoryProvider REMOTE = mock(TimeLockPaxosHistoryProvider.class);
    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("Bearer q");

    private DataSource dataSource;
    private PaxosStateLog<PaxosValue> learnerLog;
    private PaxosStateLog<PaxosAcceptorState> acceptorLog;
    private LocalHistoryLoader history;
    private TimeLockPaxosHistoryProviderResource resource;
    private PaxosLogHistoryProvider paxosLogHistoryProvider;
    private LogVerificationProgressState verificationProgressState;

    @Before
    public void setup() {
        dataSource = SqliteConnections.getPooledDataSource(tempFolder.getRoot().toPath());

        learnerLog = SqlitePaxosStateLog.create(
                ImmutableNamespaceAndUseCase.of(CLIENT, USE_CASE_LEARNER), dataSource);
        acceptorLog = SqlitePaxosStateLog.create(
                ImmutableNamespaceAndUseCase.of(CLIENT, USE_CASE_ACCEPTOR), dataSource);

        history = LocalHistoryLoader.create(SqlitePaxosStateLogHistory.create(dataSource));
        resource = new TimeLockPaxosHistoryProviderResource(history);

        verificationProgressState = LogVerificationProgressState.create(dataSource);
        paxosLogHistoryProvider = new PaxosLogHistoryProvider(dataSource, ImmutableList.of(REMOTE));
    }

    @Test
    public void canFetchAndCombineHistoriesForLocalAndRemote() {
        writeToLogs(acceptorLog, learnerLog, 100);
        int lastVerified = -1;

        List<HistoryQuery> historyQueries = ImmutableList.of(HistoryQuery.of(
                ImmutableNamespaceAndUseCase.of(CLIENT, USE_CASE), lastVerified));

        List<LogsForNamespaceAndUseCase> remoteHistory
                = AtlasFutures.getUnchecked(resource.getPaxosHistory(AUTH_HEADER,
                historyQueries));

        when(REMOTE.getPaxosHistory(any(), any())).thenReturn(remoteHistory);

        List<CompletePaxosHistoryForNamespaceAndUseCase> completeHistory = paxosLogHistoryProvider.getHistory();

        assertThat(completeHistory.size()).isEqualTo(1);
        CompletePaxosHistoryForNamespaceAndUseCase history = completeHistory.get(0);

        assertThat(history.namespace()).isEqualTo(CLIENT);
        assertThat(history.useCase()).isEqualTo(USE_CASE);
        assertThat(history.localAndRemoteLearnerAndAcceptorRecords().size()).isEqualTo(2);
        assertThat(history.localAndRemoteLearnerAndAcceptorRecords().get(0).size()).isEqualTo(100);
        assertThat(history.localAndRemoteLearnerAndAcceptorRecords().get(1).size()).isEqualTo(100);
    }

    @Test
    public void throwsIfRemoteThrows() {
        writeToLogs(acceptorLog, learnerLog, 100);
        when(REMOTE.getPaxosHistory(any(), any())).thenThrow(new RuntimeException());
        assertThatThrownBy(paxosLogHistoryProvider::getHistory)
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    public void canFetchAndCombineHistoriesSinceLastVerifiedState() {
        writeToLogs(acceptorLog, learnerLog, 100);
        int lastVerified = 17;
        verificationProgressState.updateProgress(CLIENT, USE_CASE, lastVerified);

        List<HistoryQuery> historyQueries = ImmutableList.of(HistoryQuery.of(
                ImmutableNamespaceAndUseCase.of(CLIENT, USE_CASE), lastVerified));
        List<LogsForNamespaceAndUseCase> remoteHistory
                = AtlasFutures.getUnchecked(resource.getPaxosHistory(AUTH_HEADER,
                historyQueries));
        when(REMOTE.getPaxosHistory(any(), any())).thenReturn(remoteHistory);

        List<CompletePaxosHistoryForNamespaceAndUseCase> completeHistory = paxosLogHistoryProvider.getHistory();
        assertThat(completeHistory.size()).isEqualTo(1);
        CompletePaxosHistoryForNamespaceAndUseCase history = completeHistory.get(0);

        assertThat(history.namespace()).isEqualTo(CLIENT);
        assertThat(history.useCase()).isEqualTo(USE_CASE);
        assertThat(history.localAndRemoteLearnerAndAcceptorRecords().size()).isEqualTo(2);
        assertThat(history.localAndRemoteLearnerAndAcceptorRecords().get(0).size()).isEqualTo(100 - lastVerified);
        assertThat(history.localAndRemoteLearnerAndAcceptorRecords().get(1).size()).isEqualTo(100 - lastVerified);
    }
}
