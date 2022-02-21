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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.Client;
import com.palantir.paxos.ImmutableNamespaceAndUseCase;
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.paxos.PaxosAcceptorState;
import com.palantir.paxos.PaxosStateLog;
import com.palantir.paxos.PaxosValue;
import com.palantir.paxos.SqliteConnections;
import com.palantir.paxos.SqlitePaxosStateLog;
import com.palantir.timelock.history.models.AcceptorUseCase;
import com.palantir.timelock.history.models.CompletePaxosHistoryForNamespaceAndUseCase;
import com.palantir.timelock.history.models.ConsolidatedLearnerAndAcceptorRecord;
import com.palantir.timelock.history.models.LearnedAndAcceptedValue;
import com.palantir.timelock.history.models.LearnerUseCase;
import com.palantir.timelock.history.remote.HistoryLoaderAndTransformer;
import com.palantir.timelock.history.sqlite.SqlitePaxosStateLogHistory;
import com.palantir.timelock.history.utils.HistoryQueries;
import com.palantir.timelock.history.utils.PaxosSerializationTestUtils;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PaxosLogHistoryProviderTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final String USE_CASE_LEARNER =
            LearnerUseCase.createLearnerUseCase(DEFAULT_USE_CASE).value();
    private static final String USE_CASE_ACCEPTOR =
            AcceptorUseCase.createAcceptorUseCase(DEFAULT_USE_CASE).value();

    private TimeLockPaxosHistoryProvider remote;
    private DataSource dataSource;
    private PaxosStateLog<PaxosValue> learnerLog;
    private PaxosStateLog<PaxosAcceptorState> acceptorLog;
    private LocalHistoryLoader history;
    private PaxosLogHistoryProvider paxosLogHistoryProvider;
    private PaxosLogHistoryProgressTracker progressTracker;

    @Before
    public void setup() {
        remote = mock(TimeLockPaxosHistoryProvider.class);
        dataSource = SqliteConnections.getDefaultConfiguredPooledDataSource(
                tempFolder.getRoot().toPath());

        learnerLog = createLearnerLog(ImmutableNamespaceAndUseCase.of(DEFAULT_CLIENT, USE_CASE_LEARNER));
        acceptorLog = createAcceptorLog(ImmutableNamespaceAndUseCase.of(DEFAULT_CLIENT, USE_CASE_ACCEPTOR));

        history = LocalHistoryLoader.create(SqlitePaxosStateLogHistory.create(dataSource));
        progressTracker = new PaxosLogHistoryProgressTracker(dataSource, SqlitePaxosStateLogHistory.create(dataSource));
        paxosLogHistoryProvider = new PaxosLogHistoryProvider(dataSource, ImmutableList.of(remote));
    }

    @Test
    public void canFetchAndCombineHistoriesForLocalAndRemote() {
        Set<PaxosValue> paxosValues = PaxosSerializationTestUtils.writeToLogs(acceptorLog, learnerLog, 1, 100);

        int lastVerified = -1;

        List<HistoryQuery> historyQueries =
                ImmutableList.of(HistoryQueries.unboundedHistoryQuerySinceSeq(lastVerified));

        List<LogsForNamespaceAndUseCase> remoteHistory =
                HistoryLoaderAndTransformer.getLogsForHistoryQueries(history, historyQueries);

        when(remote.getPaxosHistory(any(), any())).thenReturn(PaxosHistoryOnRemote.of(remoteHistory));

        List<CompletePaxosHistoryForNamespaceAndUseCase> completeHistory = paxosLogHistoryProvider.getHistory();

        CompletePaxosHistoryForNamespaceAndUseCase historyForNamespaceAndUseCase =
                Iterables.getOnlyElement(completeHistory);
        assertThat(historyForNamespaceAndUseCase.namespace()).isEqualTo(DEFAULT_CLIENT);
        assertSanityWithValuesOfFetchedRecords(
                historyForNamespaceAndUseCase, DEFAULT_CLIENT, DEFAULT_USE_CASE, 100, paxosValues);
    }

    @Test
    public void canFetchAndCombineDiscontinuousLogs() {
        Set<PaxosValue> paxosValues1 = PaxosSerializationTestUtils.writeToLogs(acceptorLog, learnerLog, 7, 54);
        Set<PaxosValue> paxosValues2 = PaxosSerializationTestUtils.writeToLogs(acceptorLog, learnerLog, 98, 127);

        Set<PaxosValue> paxosValues = ImmutableSet.<PaxosValue>builder()
                .addAll(paxosValues1)
                .addAll(paxosValues2)
                .build();

        int lastVerified = -1;

        List<HistoryQuery> historyQueries =
                ImmutableList.of(HistoryQueries.unboundedHistoryQuerySinceSeq(lastVerified));

        List<LogsForNamespaceAndUseCase> remoteHistory =
                HistoryLoaderAndTransformer.getLogsForHistoryQueries(history, historyQueries);

        when(remote.getPaxosHistory(any(), any())).thenReturn(PaxosHistoryOnRemote.of(remoteHistory));

        List<CompletePaxosHistoryForNamespaceAndUseCase> completeHistory = paxosLogHistoryProvider.getHistory();

        CompletePaxosHistoryForNamespaceAndUseCase historyForNamespaceAndUseCase =
                Iterables.getOnlyElement(completeHistory);
        assertThat(historyForNamespaceAndUseCase.namespace()).isEqualTo(DEFAULT_CLIENT);
        assertSanityWithValuesOfFetchedRecords(
                historyForNamespaceAndUseCase,
                DEFAULT_CLIENT,
                DEFAULT_USE_CASE,
                (54 - 7 + 1) + (127 - 98 + 1),
                paxosValues);
    }

    @Test
    public void throwsIfRemoteThrows() {
        PaxosSerializationTestUtils.writeToLogs(acceptorLog, learnerLog, 1, 100);
        when(remote.getPaxosHistory(any(), any())).thenThrow(new RuntimeException());
        assertThatThrownBy(paxosLogHistoryProvider::getHistory).isInstanceOf(RuntimeException.class);
    }

    @Test
    public void canFetchAndCombineHistoriesSinceLastVerifiedState() {
        Set<PaxosValue> paxosValues = PaxosSerializationTestUtils.writeToLogs(acceptorLog, learnerLog, 1, 100);
        int lastVerified = 17;

        Set<PaxosValue> expectedPaxosValues = paxosValues.stream()
                .filter(val -> val.getRound() > lastVerified)
                .collect(Collectors.toSet());

        progressTracker.updateProgressStateForNamespaceAndUseCase(
                DEFAULT_NAMESPACE_AND_USE_CASE, HistoryQuerySequenceBounds.of(-1, lastVerified));
        HistoryQuerySequenceBounds bounds =
                progressTracker.getNextPaxosLogSequenceRangeToBeVerified(DEFAULT_NAMESPACE_AND_USE_CASE);

        List<HistoryQuery> historyQueries =
                ImmutableList.of(HistoryQueries.unboundedHistoryQuerySinceSeq(bounds.getLowerBoundInclusive()));
        List<LogsForNamespaceAndUseCase> remoteHistory =
                HistoryLoaderAndTransformer.getLogsForHistoryQueries(history, historyQueries);

        when(remote.getPaxosHistory(any(), any())).thenReturn(PaxosHistoryOnRemote.of(remoteHistory));

        List<CompletePaxosHistoryForNamespaceAndUseCase> completeHistory = paxosLogHistoryProvider.getHistory();

        CompletePaxosHistoryForNamespaceAndUseCase historyForNamespaceAndUseCase =
                Iterables.getOnlyElement(completeHistory);

        assertSanityWithValuesOfFetchedRecords(
                historyForNamespaceAndUseCase,
                DEFAULT_CLIENT,
                DEFAULT_USE_CASE,
                100 - lastVerified,
                expectedPaxosValues);
    }

    @Test
    public void canFetchAndCombineHistoriesAcrossNamespaceAndUseCasePairs() {
        Map<NamespaceAndUseCase, Set<PaxosValue>> expected = writeLogsForRangeOfNamespaceUseCasePairs();
        Set<NamespaceAndUseCase> allNamespaceAndUseCases = expected.keySet();

        List<HistoryQuery> historyQueries = allNamespaceAndUseCases.stream()
                .map(namespaceAndUseCase ->
                        HistoryQueries.unboundedHistoryQuerySinceSeqForNamespaceAndUseCase(namespaceAndUseCase, -1))
                .collect(Collectors.toList());

        List<LogsForNamespaceAndUseCase> remoteHistory =
                HistoryLoaderAndTransformer.getLogsForHistoryQueries(history, historyQueries);
        when(remote.getPaxosHistory(any(), any())).thenReturn(PaxosHistoryOnRemote.of(remoteHistory));

        List<CompletePaxosHistoryForNamespaceAndUseCase> completeHistory = paxosLogHistoryProvider.getHistory();
        assertThat(completeHistory).hasSize(100);

        Set<NamespaceAndUseCase> namespaceAndUseCasesWithHistory = completeHistory.stream()
                .map(historyForNamespaceAndUseCase -> {
                    Client client = historyForNamespaceAndUseCase.namespace();
                    String useCase = historyForNamespaceAndUseCase.useCase();
                    NamespaceAndUseCase namespaceAndUseCase = ImmutableNamespaceAndUseCase.of(client, useCase);

                    assertSanityWithValuesOfFetchedRecords(
                            historyForNamespaceAndUseCase,
                            client,
                            useCase,
                            getIntegerValueOfClient(historyForNamespaceAndUseCase),
                            expected.get(namespaceAndUseCase));
                    return namespaceAndUseCase;
                })
                .collect(Collectors.toSet());

        assertThat(namespaceAndUseCasesWithHistory).containsExactlyInAnyOrderElementsOf(allNamespaceAndUseCases);
    }

    // utils
    private Map<NamespaceAndUseCase, Set<PaxosValue>> writeLogsForRangeOfNamespaceUseCasePairs() {
        return KeyedStream.of(IntStream.rangeClosed(1, 100).boxed())
                .mapEntries((idx, unused) -> {
                    String useCase = String.valueOf(idx);
                    Client client = Client.of(useCase);

                    Set<PaxosValue> paxosValues = PaxosSerializationTestUtils.writeToLogs(
                            createAcceptorLog(ImmutableNamespaceAndUseCase.of(
                                    client,
                                    AcceptorUseCase.createAcceptorUseCase(useCase)
                                            .value())),
                            createLearnerLog(ImmutableNamespaceAndUseCase.of(
                                    client,
                                    LearnerUseCase.createLearnerUseCase(useCase).value())),
                            1,
                            idx);

                    NamespaceAndUseCase namespaceAndUseCase = ImmutableNamespaceAndUseCase.of(client, useCase);
                    return Maps.immutableEntry(namespaceAndUseCase, paxosValues);
                })
                .collectToMap();
    }

    private PaxosStateLog<PaxosValue> createLearnerLog(NamespaceAndUseCase namespaceAndUseCase) {
        return SqlitePaxosStateLog.create(namespaceAndUseCase, dataSource);
    }

    private PaxosStateLog<PaxosAcceptorState> createAcceptorLog(NamespaceAndUseCase namespaceAndUseCase) {
        return SqlitePaxosStateLog.create(namespaceAndUseCase, dataSource);
    }

    private void assertSanityWithValuesOfFetchedRecords(
            CompletePaxosHistoryForNamespaceAndUseCase historyForNamespaceAndUseCase,
            Client client,
            String useCase,
            int numberOfLogs,
            Set<PaxosValue> expectedValues) {

        List<ConsolidatedLearnerAndAcceptorRecord> localAndRemoteLearnerAndAcceptorRecords =
                assertSanityOfRecords(historyForNamespaceAndUseCase, client, useCase, numberOfLogs);

        Map<Long, LearnedAndAcceptedValue> record =
                localAndRemoteLearnerAndAcceptorRecords.get(0).record();
        List<PaxosValue> paxosValues = record.values().stream()
                .map(LearnedAndAcceptedValue::learnedValue)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        assertThat(paxosValues).hasSameElementsAs(expectedValues);
    }

    private List<ConsolidatedLearnerAndAcceptorRecord> assertSanityOfRecords(
            CompletePaxosHistoryForNamespaceAndUseCase historyForNamespaceAndUseCase,
            Client client,
            String useCase,
            int numberOfLogs) {
        assertThat(historyForNamespaceAndUseCase.useCase()).isEqualTo(useCase);
        assertThat(historyForNamespaceAndUseCase.namespace()).isEqualTo(client);

        List<ConsolidatedLearnerAndAcceptorRecord> localAndRemoteLearnerAndAcceptorRecords =
                historyForNamespaceAndUseCase.localAndRemoteLearnerAndAcceptorRecords();

        assertThat(localAndRemoteLearnerAndAcceptorRecords).hasSize(2); // there is one local and one remote

        assertThat(localAndRemoteLearnerAndAcceptorRecords)
                .allMatch(r -> r.record().size() == numberOfLogs);
        return localAndRemoteLearnerAndAcceptorRecords;
    }

    private int getIntegerValueOfClient(CompletePaxosHistoryForNamespaceAndUseCase historyForNamespaceAndUseCase) {
        return Integer.parseInt(historyForNamespaceAndUseCase.namespace().value());
    }
}
