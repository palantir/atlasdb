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

import static com.palantir.timelock.TimelockCorruptionTestConstants.DEFAULT_CLIENT;
import static com.palantir.timelock.TimelockCorruptionTestConstants.DEFAULT_NAMESPACE_AND_USE_CASE;
import static com.palantir.timelock.TimelockCorruptionTestConstants.DEFAULT_USE_CASE;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.palantir.paxos.ImmutableNamespaceAndUseCase;
import com.palantir.paxos.PaxosAcceptorState;
import com.palantir.paxos.PaxosStateLog;
import com.palantir.paxos.PaxosValue;
import com.palantir.paxos.SqliteConnections;
import com.palantir.paxos.SqlitePaxosStateLog;
import com.palantir.timelock.history.models.AcceptorUseCase;
import com.palantir.timelock.history.models.LearnerUseCase;
import com.palantir.timelock.history.remote.HistoryLoaderAndTransformer;
import com.palantir.timelock.history.sqlite.SqlitePaxosStateLogHistory;
import com.palantir.timelock.history.utils.HistoryQueries;
import com.palantir.timelock.history.utils.PaxosSerializationTestUtils;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class HistoryLoaderAndTransformerTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private PaxosStateLog<PaxosValue> learnerLog;
    private PaxosStateLog<PaxosAcceptorState> acceptorLog;
    private LocalHistoryLoader history;

    @Before
    public void setup() {
        DataSource dataSource =
                SqliteConnections.getDefaultConfiguredPooledDataSource(tempFolder.getRoot().toPath());
        learnerLog = SqlitePaxosStateLog.create(
                ImmutableNamespaceAndUseCase.of(
                        DEFAULT_CLIENT,
                        LearnerUseCase.createLearnerUseCase(DEFAULT_USE_CASE).value()),
                dataSource);
        acceptorLog = SqlitePaxosStateLog.create(
                ImmutableNamespaceAndUseCase.of(
                        DEFAULT_CLIENT,
                        AcceptorUseCase.createAcceptorUseCase(DEFAULT_USE_CASE).value()),
                dataSource);
        history = LocalHistoryLoader.create(SqlitePaxosStateLogHistory.create(dataSource));
    }

    @Test
    public void canFetchLogsForQuery() {
        writeToLogs(1, 100);
        int lastVerified = 27;
        List<HistoryQuery> historyQueries =
                ImmutableList.of(HistoryQueries.unboundedHistoryQuerySinceSeq(lastVerified));
        List<LogsForNamespaceAndUseCase> paxosHistory =
                HistoryLoaderAndTransformer.getLogsForHistoryQueries(history, historyQueries);

        sanityCheckLoadedHistory(paxosHistory, 100 - lastVerified + 1);
    }

    @Test
    public void canHandleDuplicateQueries() {
        writeToLogs(1, 100);
        int minLastVerified = 27;

        List<HistoryQuery> queries = IntStream.range(0, 10)
                .boxed()
                .map(idx -> HistoryQueries.unboundedHistoryQuerySinceSeq(minLastVerified))
                .collect(Collectors.toList());

        List<LogsForNamespaceAndUseCase> paxosHistory =
                HistoryLoaderAndTransformer.getLogsForHistoryQueries(history, queries);

        sanityCheckLoadedHistory(paxosHistory, 100 - minLastVerified + 1);
    }

    @Test
    public void canHandleHistoryWithOnlyAcceptorLogs() {
        IntStream.range(0, 100).forEach(i -> {
            PaxosSerializationTestUtils.writeAcceptorStateForLogAndRound(acceptorLog, i + 1, Optional.empty());
        });

        int lastVerified = 27;
        List<HistoryQuery> historyQueries =
                ImmutableList.of(HistoryQueries.unboundedHistoryQuerySinceSeq(lastVerified));

        List<LogsForNamespaceAndUseCase> paxosHistory =
                HistoryLoaderAndTransformer.getLogsForHistoryQueries(history, historyQueries);

        LogsForNamespaceAndUseCase logsForNamespaceAndUseCase =
                sanityCheckLoadedHistory(paxosHistory, 100 - lastVerified + 1);
        PaxosLogWithAcceptedAndLearnedValues singleLog =
                logsForNamespaceAndUseCase.getLogs().get(0);
        assertThat(singleLog.getAcceptedState()).isPresent();
        assertThat(singleLog.getPaxosValue()).isNotPresent();
    }

    @Test
    public void canHandleHistoryWithOnlyLearnerLogs() {
        IntStream.range(0, 100).forEach(i -> {
            PaxosSerializationTestUtils.createAndWriteValueForLogAndRound(learnerLog, i + 1);
        });

        int lastVerified = 52;
        List<HistoryQuery> historyQueries =
                ImmutableList.of(HistoryQueries.unboundedHistoryQuerySinceSeq(lastVerified));

        List<LogsForNamespaceAndUseCase> paxosHistory =
                HistoryLoaderAndTransformer.getLogsForHistoryQueries(history, historyQueries);

        LogsForNamespaceAndUseCase logsForNamespaceAndUseCase =
                sanityCheckLoadedHistory(paxosHistory, 100 - lastVerified + 1);
        PaxosLogWithAcceptedAndLearnedValues singleLog =
                logsForNamespaceAndUseCase.getLogs().get(0);
        assertThat(singleLog.getAcceptedState()).isNotPresent();
        assertThat(singleLog.getPaxosValue()).isPresent();
    }

    @Test
    public void canHandleHistoryWithNoLogs() {
        int lastVerified = 102;
        List<HistoryQuery> historyQueries =
                ImmutableList.of(HistoryQueries.unboundedHistoryQuerySinceSeq(lastVerified));

        List<LogsForNamespaceAndUseCase> paxosHistory =
                HistoryLoaderAndTransformer.getLogsForHistoryQueries(history, historyQueries);

        sanityCheckLoadedHistory(paxosHistory, 0);
    }

    @Test
    public void canHandleHistoryWithDiscontinuousLogs() {
        int lastVerified = 3;
        int firstSeqWithLog = 45;

        writeToLogs(firstSeqWithLog, 100);

        List<HistoryQuery> historyQueries =
                ImmutableList.of(HistoryQueries.unboundedHistoryQuerySinceSeq(lastVerified));
        List<LogsForNamespaceAndUseCase> paxosHistory =
                HistoryLoaderAndTransformer.getLogsForHistoryQueries(history, historyQueries);

        sanityCheckLoadedHistory(paxosHistory, 100 - firstSeqWithLog + 1);
    }

    private LogsForNamespaceAndUseCase sanityCheckLoadedHistory(
            List<LogsForNamespaceAndUseCase> paxosHistory, int logCount) {

        assertThat(paxosHistory).hasSize(1);

        LogsForNamespaceAndUseCase logsForNamespaceAndUseCase = paxosHistory.get(0);
        assertThat(logsForNamespaceAndUseCase.getNamespaceAndUseCase()).isEqualTo(DEFAULT_NAMESPACE_AND_USE_CASE);
        assertThat(logsForNamespaceAndUseCase.getLogs()).hasSize(logCount);

        return logsForNamespaceAndUseCase;
    }

    private void writeToLogs(int start, int end) {
        PaxosSerializationTestUtils.writeToLogs(acceptorLog, learnerLog, start, end);
    }
}
