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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableList;
import com.palantir.timelock.history.remote.HistoryLoaderAndTransformer;
import com.palantir.timelock.history.sqlite.SqlitePaxosStateLogHistory;
import com.palantir.timelock.history.util.UseCaseUtils;
import com.palantir.paxos.Client;
import com.palantir.paxos.ImmutableNamespaceAndUseCase;
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.paxos.PaxosAcceptorState;
import com.palantir.paxos.PaxosStateLog;
import com.palantir.paxos.PaxosValue;
import com.palantir.paxos.SqliteConnections;
import com.palantir.paxos.SqlitePaxosStateLog;
import com.palantir.timelock.history.utils.PaxosSerializationTestUtils;

public class HistoryLoaderAndTransformerTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final Client CLIENT = Client.of("client");
    private static final String USE_CASE = "useCase";
    private static final NamespaceAndUseCase NAMESPACE_AND_USE_CASE = ImmutableNamespaceAndUseCase.of(CLIENT, USE_CASE);

    private PaxosStateLog<PaxosValue> learnerLog;
    private PaxosStateLog<PaxosAcceptorState> acceptorLog;
    private LocalHistoryLoader history;

    @Before
    public void setup() {
        DataSource dataSource = SqliteConnections.getPooledDataSource(tempFolder.getRoot().toPath());
        learnerLog = SqlitePaxosStateLog.create(
                ImmutableNamespaceAndUseCase.of(CLIENT, UseCaseUtils.getLearnerUseCase(USE_CASE).value()), dataSource);
        acceptorLog = SqlitePaxosStateLog.create(
                ImmutableNamespaceAndUseCase.of(CLIENT, UseCaseUtils.getAcceptorUseCase(USE_CASE).value()), dataSource);
        history = LocalHistoryLoader.create(SqlitePaxosStateLogHistory.create(dataSource));
    }

    @Test
    public void canFetchLogsForQuery() {
        writeToLogs(1, 100);
        int lastVerified = 27;
        List<HistoryQuery> historyQueries = ImmutableList.of(HistoryQuery.of(NAMESPACE_AND_USE_CASE, lastVerified));
        List<LogsForNamespaceAndUseCase> paxosHistory
                = HistoryLoaderAndTransformer.getLogsForHistoryQueries(history, historyQueries);

        sanityCheckLoadedHistory(paxosHistory, 100 - lastVerified);
    }

    @Test
    public void canHandleDuplicateQueries() {
        writeToLogs(1, 100);
        int minLastVerified = 27;

        List<HistoryQuery> queries = IntStream.range(0, 10).boxed().map(
                idx -> HistoryQuery.of(NAMESPACE_AND_USE_CASE, minLastVerified + idx))
                .collect(Collectors.toList());

        List<LogsForNamespaceAndUseCase> paxosHistory
                = HistoryLoaderAndTransformer.getLogsForHistoryQueries(history, queries);

        sanityCheckLoadedHistory(paxosHistory, 100 - minLastVerified);
    }

    @Test
    public void canHandleHistoryWithOnlyAcceptorLogs() {
        IntStream.range(0, 100).forEach(i -> {
            PaxosSerializationTestUtils.writeAcceptorStateForLogAndRound(acceptorLog, i + 1);
        });

        int lastVerified = 27;
        List<HistoryQuery> historyQueries = ImmutableList.of(HistoryQuery.of(NAMESPACE_AND_USE_CASE, lastVerified));

        List<LogsForNamespaceAndUseCase> paxosHistory
                = HistoryLoaderAndTransformer.getLogsForHistoryQueries(history, historyQueries);

        LogsForNamespaceAndUseCase logsForNamespaceAndUseCase
                = sanityCheckLoadedHistory(paxosHistory, 100 - lastVerified);
        PaxosLogWithAcceptedAndLearnedValues singleLog = logsForNamespaceAndUseCase.getLogs().get(0);
        assertThat(singleLog.getAcceptedState()).isPresent();
        assertThat(singleLog.getPaxosValue()).isNotPresent();
    }

    @Test
    public void canHandleHistoryWithOnlyLearnerLogs() {
        IntStream.range(0, 100).forEach(i -> {
            PaxosSerializationTestUtils.writeValueForLogAndRound(learnerLog, i + 1);
        });

        int lastVerified = 52;
        List<HistoryQuery> historyQueries = ImmutableList.of(HistoryQuery.of(NAMESPACE_AND_USE_CASE, lastVerified));

        List<LogsForNamespaceAndUseCase> paxosHistory
                = HistoryLoaderAndTransformer.getLogsForHistoryQueries(history, historyQueries);

        LogsForNamespaceAndUseCase logsForNamespaceAndUseCase
                = sanityCheckLoadedHistory(paxosHistory, 100 - lastVerified);
        PaxosLogWithAcceptedAndLearnedValues singleLog = logsForNamespaceAndUseCase.getLogs().get(0);
        assertThat(singleLog.getAcceptedState()).isNotPresent();
        assertThat(singleLog.getPaxosValue()).isPresent();
    }

    @Test
    public void canHandleHistoryWithNoLogs() {
        int lastVerified = 102;
        List<HistoryQuery> historyQueries = ImmutableList.of(HistoryQuery.of(NAMESPACE_AND_USE_CASE, lastVerified));

        List<LogsForNamespaceAndUseCase> paxosHistory
                = HistoryLoaderAndTransformer.getLogsForHistoryQueries(history, historyQueries);

        sanityCheckLoadedHistory(paxosHistory, 0);
    }

    @Test
    public void canHandleHistoryWithDiscontinuousLogs() {
        int lastVerified = 3;
        int firstSeqWithLog = 45;

        writeToLogs(firstSeqWithLog, 100);

        List<HistoryQuery> historyQueries = ImmutableList.of(HistoryQuery.of(NAMESPACE_AND_USE_CASE, lastVerified));
        List<LogsForNamespaceAndUseCase> paxosHistory
                = HistoryLoaderAndTransformer.getLogsForHistoryQueries(history, historyQueries);

        sanityCheckLoadedHistory(paxosHistory, (100 - firstSeqWithLog + 1));
    }


    private LogsForNamespaceAndUseCase sanityCheckLoadedHistory(
            List<LogsForNamespaceAndUseCase> paxosHistory, int logCount) {

        assertThat(paxosHistory.size()).isEqualTo(1);

        LogsForNamespaceAndUseCase logsForNamespaceAndUseCase = paxosHistory.get(0);
        assertThat(logsForNamespaceAndUseCase.getNamespaceAndUseCase()).isEqualTo(NAMESPACE_AND_USE_CASE);
        assertThat(logsForNamespaceAndUseCase.getLogs().size()).isEqualTo(logCount);

        return logsForNamespaceAndUseCase;
    }

    private void writeToLogs(int start, int end) {
        IntStream.rangeClosed(start, end).forEach(i -> {
            PaxosSerializationTestUtils.writeAcceptorStateForLogAndRound(acceptorLog, i);
            PaxosSerializationTestUtils.writeValueForLogAndRound(learnerLog, i);
        });
    }
}
