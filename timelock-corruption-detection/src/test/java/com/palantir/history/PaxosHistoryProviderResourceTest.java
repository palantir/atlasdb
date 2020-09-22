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

import static com.palantir.history.utils.Utils.writeAcceptorStateForLogAndRound;
import static com.palantir.history.utils.Utils.writeToLogs;
import static com.palantir.history.utils.Utils.writeValueForLogAndRound;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.futures.AtlasFutures;
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
import com.palantir.timelock.history.PaxosLogWithAcceptedAndLearnedValues;
import com.palantir.tokens.auth.AuthHeader;

public class PaxosHistoryProviderResourceTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final Client CLIENT = Client.of("client");
    private static final String USE_CASE = "useCase";
    private static final String USE_CASE_LEARNER = "useCase!learner";
    private static final String USE_CASE_ACCEPTOR = "useCase!acceptor";
    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("Bearer q");

    private DataSource dataSource;
    private PaxosStateLog<PaxosValue> learnerLog;
    private PaxosStateLog<PaxosAcceptorState> acceptorLog;
    private LocalHistoryLoader history;
    private TimeLockPaxosHistoryProviderResource resource;

    @Before
    public void setup() {
        dataSource = SqliteConnections.getPooledDataSource(tempFolder.getRoot().toPath());
        learnerLog = SqlitePaxosStateLog.create(
                ImmutableNamespaceAndUseCase.of(CLIENT, USE_CASE_LEARNER), dataSource);
        acceptorLog = SqlitePaxosStateLog.create(
                ImmutableNamespaceAndUseCase.of(CLIENT, USE_CASE_ACCEPTOR), dataSource);
        history = LocalHistoryLoader.create(SqlitePaxosStateLogHistory.create(dataSource));
        resource = new TimeLockPaxosHistoryProviderResource(history);
    }

    @Test
    public void canFetchLogsForQuery() {
        writeToLogs(acceptorLog, learnerLog, 100);
        int lastVerified = 27;
        List<HistoryQuery> historyQueries = ImmutableList.of(HistoryQuery.of(
                ImmutableNamespaceAndUseCase.of(CLIENT, USE_CASE), lastVerified));
        List<LogsForNamespaceAndUseCase> paxosHistory
                = AtlasFutures.getUnchecked(resource.getPaxosHistory(AUTH_HEADER, historyQueries));

        assertThat(paxosHistory.size()).isEqualTo(1);
        LogsForNamespaceAndUseCase logsForNamespaceAndUseCase = paxosHistory.get(0);

        assertThat(logsForNamespaceAndUseCase.getNamespaceAndUseCase().namespace()).isEqualTo(CLIENT);
        assertThat(logsForNamespaceAndUseCase.getNamespaceAndUseCase().useCase()).isEqualTo(USE_CASE);
        assertThat(logsForNamespaceAndUseCase.getLogs().size()).isEqualTo(100 - lastVerified);
    }

    @Test
    public void canHandleDuplicateQueries() {
        writeToLogs(acceptorLog, learnerLog, 100);
        int minLastVerified = 27;

        List<HistoryQuery> queries = IntStream.range(0, 10).boxed().map(
                idx -> HistoryQuery.of(ImmutableNamespaceAndUseCase.of(CLIENT, USE_CASE), minLastVerified + idx))
                .collect(Collectors.toList());

        List<LogsForNamespaceAndUseCase> paxosHistory
                = AtlasFutures.getUnchecked(resource.getPaxosHistory(AUTH_HEADER, queries));

        assertThat(paxosHistory.size()).isEqualTo(1);
        LogsForNamespaceAndUseCase logsForNamespaceAndUseCase = paxosHistory.get(0);

        assertThat(logsForNamespaceAndUseCase.getNamespaceAndUseCase().namespace()).isEqualTo(CLIENT);
        assertThat(logsForNamespaceAndUseCase.getNamespaceAndUseCase().useCase()).isEqualTo(USE_CASE);
        assertThat(logsForNamespaceAndUseCase.getLogs().size()).isEqualTo(100 - minLastVerified);
    }

    @Test
    public void canHandleHistoryWithOnlyAcceptorLogs() {
        IntStream.range(0, 100).forEach(i -> {
            writeAcceptorStateForLogAndRound(acceptorLog, i + 1);
        });

        int lastVerified = 27;
        List<HistoryQuery> historyQueries = ImmutableList.of(HistoryQuery.of(
                ImmutableNamespaceAndUseCase.of(CLIENT, USE_CASE), lastVerified));

        List<LogsForNamespaceAndUseCase> paxosHistory
                = AtlasFutures.getUnchecked(resource.getPaxosHistory(AUTH_HEADER, historyQueries));

        assertThat(paxosHistory.size()).isEqualTo(1);
        LogsForNamespaceAndUseCase logsForNamespaceAndUseCase = paxosHistory.get(0);

        assertThat(logsForNamespaceAndUseCase.getNamespaceAndUseCase().namespace()).isEqualTo(CLIENT);
        assertThat(logsForNamespaceAndUseCase.getNamespaceAndUseCase().useCase()).isEqualTo(USE_CASE);
        assertThat(logsForNamespaceAndUseCase.getLogs().size()).isEqualTo(100 - lastVerified);

        PaxosLogWithAcceptedAndLearnedValues singleLog = logsForNamespaceAndUseCase.getLogs().get(0);
        assertThat(singleLog.getAcceptedState()).isPresent();
        assertThat(singleLog.getPaxosValue()).isNotPresent();
    }

    @Test
    public void canHandleHistoryWithOnlyLearnerLogs() {
        IntStream.range(0, 100).forEach(i -> {
            writeValueForLogAndRound(learnerLog, i + 1);
        });

        int lastVerified = 52;
        List<HistoryQuery> historyQueries = ImmutableList.of(HistoryQuery.of(
                ImmutableNamespaceAndUseCase.of(CLIENT, USE_CASE), lastVerified));

        List<LogsForNamespaceAndUseCase> paxosHistory
                = AtlasFutures.getUnchecked(resource.getPaxosHistory(AUTH_HEADER, historyQueries));

        assertThat(paxosHistory.size()).isEqualTo(1);
        LogsForNamespaceAndUseCase logsForNamespaceAndUseCase = paxosHistory.get(0);

        assertThat(logsForNamespaceAndUseCase.getNamespaceAndUseCase().namespace()).isEqualTo(CLIENT);
        assertThat(logsForNamespaceAndUseCase.getNamespaceAndUseCase().useCase()).isEqualTo(USE_CASE);
        assertThat(logsForNamespaceAndUseCase.getLogs().size()).isEqualTo(100 - lastVerified);

        PaxosLogWithAcceptedAndLearnedValues singleLog = logsForNamespaceAndUseCase.getLogs().get(0);
        assertThat(singleLog.getAcceptedState()).isNotPresent();
        assertThat(singleLog.getPaxosValue()).isPresent();
    }

    @Test
    public void canHandleHistoryWithNoLogs() {
        int lastVerified = 102;
        List<HistoryQuery> historyQueries = ImmutableList.of(HistoryQuery.of(
                ImmutableNamespaceAndUseCase.of(CLIENT, USE_CASE), lastVerified));

        List<LogsForNamespaceAndUseCase> paxosHistory
                = AtlasFutures.getUnchecked(resource.getPaxosHistory(AUTH_HEADER, historyQueries));

        assertThat(paxosHistory.size()).isEqualTo(1);
        LogsForNamespaceAndUseCase logsForNamespaceAndUseCase = paxosHistory.get(0);

        assertThat(logsForNamespaceAndUseCase.getNamespaceAndUseCase().namespace()).isEqualTo(CLIENT);
        assertThat(logsForNamespaceAndUseCase.getNamespaceAndUseCase().useCase()).isEqualTo(USE_CASE);
        assertThat(logsForNamespaceAndUseCase.getLogs().size()).isEqualTo(0);
    }
}
