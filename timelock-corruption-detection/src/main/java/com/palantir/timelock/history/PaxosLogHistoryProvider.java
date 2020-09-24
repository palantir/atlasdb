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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.ImmutableNamespaceAndUseCase;
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.timelock.history.models.ConsolidatedPaxosHistoryOnSingleNode;
import com.palantir.timelock.history.models.CompletePaxosHistoryForNamespaceAndUseCase;
import com.palantir.timelock.history.models.ConsolidatedLearnerAndAcceptorRecord;
import com.palantir.timelock.history.models.ImmutableCompletePaxosHistoryForNamespaceAndUseCase;
import com.palantir.timelock.history.models.ImmutableLearnedAndAcceptedValue;
import com.palantir.timelock.history.models.LearnedAndAcceptedValue;
import com.palantir.timelock.history.models.PaxosHistoryOnSingleNode;
import com.palantir.timelock.history.sqlite.LogVerificationProgressState;
import com.palantir.timelock.history.sqlite.SqlitePaxosStateLogHistory;
import com.palantir.timelock.history.util.UseCaseUtils;
import com.palantir.tokens.auth.AuthHeader;

public class PaxosLogHistoryProvider {
    private static final Logger log = LoggerFactory.getLogger(PaxosLogHistoryProvider.class);

    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("Bearer omitted");

    private final LogVerificationProgressState logVerificationProgressState;
    private final LocalHistoryLoader localHistoryLoader;
    private final SqlitePaxosStateLogHistory sqlitePaxosStateLogHistory;
    private final List<TimeLockPaxosHistoryProvider> remoteHistoryProviders;
    private Map<NamespaceAndUseCase, Long> verificationProgressStateCache = new ConcurrentHashMap<>();


    public PaxosLogHistoryProvider(DataSource dataSource, List<TimeLockPaxosHistoryProvider> remoteHistoryProviders) {
        this.remoteHistoryProviders = remoteHistoryProviders;
        this.sqlitePaxosStateLogHistory = SqlitePaxosStateLogHistory.create(dataSource);
        this.logVerificationProgressState = LogVerificationProgressState.create(dataSource);
        this.localHistoryLoader = LocalHistoryLoader.create(this.sqlitePaxosStateLogHistory);
    }

    private Set<NamespaceAndUseCase> getNamespaceAndUseCaseTuples() {
        return sqlitePaxosStateLogHistory.getAllNamespaceAndUseCaseTuples()
                .stream()
                .map(namespaceAndUseCase -> ImmutableNamespaceAndUseCase.of(
                        namespaceAndUseCase.namespace(),
                        UseCaseUtils.getPaxosUseCasePrefix(namespaceAndUseCase.useCase())))
                .collect(Collectors.toSet());
    }

    private Long getOrInsertVerificationState(NamespaceAndUseCase namespaceAndUseCase) {
        return logVerificationProgressState.getLastVerifiedSeq(
                namespaceAndUseCase.namespace(), namespaceAndUseCase.useCase());
    }


//     TODO(snanda): Refactor the two parts on translating PaxosHistoryOnRemote to
//      CompletePaxosHistoryForNamespaceAndUseCase to a separate component
    public List<CompletePaxosHistoryForNamespaceAndUseCase> getHistory() {
        Map<NamespaceAndUseCase, Long> lastVerifiedSequences = getNamespaceAndUseCaseToLastVerifiedSeqMap();

        PaxosHistoryOnSingleNode localPaxosHistory = localHistoryLoader.getLocalPaxosHistory(lastVerifiedSequences);

        List<HistoryQuery> historyQueries = getHistoryQueryListForRemoteServers(lastVerifiedSequences);

        List<PaxosHistoryOnRemote> rawHistoryFromAllRemotes = getHistoriesFromRemoteServers(historyQueries);

        List<ConsolidatedPaxosHistoryOnSingleNode> historyFromAllRemotes
                = buildHistoryFromRemoteResponses(rawHistoryFromAllRemotes);

        return consolidateAndGetHistoriesAcrossAllNodes(
                lastVerifiedSequences,
                localPaxosHistory,
                historyFromAllRemotes);
    }

    private List<CompletePaxosHistoryForNamespaceAndUseCase> consolidateAndGetHistoriesAcrossAllNodes(
            Map<NamespaceAndUseCase, Long> lastVerifiedSequences,
            PaxosHistoryOnSingleNode localPaxosHistory,
            List<ConsolidatedPaxosHistoryOnSingleNode> historyFromAllRemotes) {
        return lastVerifiedSequences.keySet().stream()
                .map(namespaceAndUseCase -> buildCompleteHistory(
                        namespaceAndUseCase,
                        localPaxosHistory,
                        historyFromAllRemotes))
                .collect(Collectors.toList());
    }

    private List<ConsolidatedPaxosHistoryOnSingleNode> buildHistoryFromRemoteResponses(
            List<PaxosHistoryOnRemote> rawHistoryFromAllRemotes) {
        return rawHistoryFromAllRemotes.stream()
                .map(this::buildRecordFromRemoteResponse)
                .collect(Collectors.toList());
    }

    private List<PaxosHistoryOnRemote> getHistoriesFromRemoteServers(List<HistoryQuery> historyQueries) {
        return remoteHistoryProviders.stream().map(
                remote -> fetchHistoryFromRemote(historyQueries, remote)).collect(Collectors.toList());
    }

    private List<HistoryQuery> getHistoryQueryListForRemoteServers(
            Map<NamespaceAndUseCase, Long> lastVerifiedSequences) {
        return KeyedStream.stream(lastVerifiedSequences)
                .mapEntries(this::buildHistoryQuery)
                .values()
                .collect(Collectors.toList());
    }

    private Map<NamespaceAndUseCase, Long> getNamespaceAndUseCaseToLastVerifiedSeqMap() {
        return KeyedStream
                .of(getNamespaceAndUseCaseTuples().stream())
                .map(namespaceAndUseCase -> verificationProgressStateCache.computeIfAbsent(namespaceAndUseCase,
                        this::getOrInsertVerificationState))
                .collectToMap();
    }

    private CompletePaxosHistoryForNamespaceAndUseCase buildCompleteHistory(NamespaceAndUseCase namespaceAndUseCase,
            PaxosHistoryOnSingleNode localPaxosHistory,
            List<ConsolidatedPaxosHistoryOnSingleNode> historyLogsFromRemotes) {

        ConsolidatedLearnerAndAcceptorRecord consolidatedLocalRecord
                = localPaxosHistory.getConsolidatedLocalAndRemoteRecord(namespaceAndUseCase);

        List<ConsolidatedLearnerAndAcceptorRecord> remoteHistoryLogsForNamespaceAndUseCase
                = extractRemoteHistoryLogsForNamespaceAndUseCase(namespaceAndUseCase, historyLogsFromRemotes);

        List<ConsolidatedLearnerAndAcceptorRecord> historyLogsAcrossAllNodes
                = combineLocalAndRemoteHistoryLogs(consolidatedLocalRecord, remoteHistoryLogsForNamespaceAndUseCase);

        return ImmutableCompletePaxosHistoryForNamespaceAndUseCase.of(
                namespaceAndUseCase.namespace(),
                namespaceAndUseCase.useCase(),
                historyLogsAcrossAllNodes);
    }

    private List<ConsolidatedLearnerAndAcceptorRecord> extractRemoteHistoryLogsForNamespaceAndUseCase(
            NamespaceAndUseCase namespaceAndUseCase,
            List<ConsolidatedPaxosHistoryOnSingleNode> historyLogsFromRemotes) {
        return historyLogsFromRemotes
                .stream()
                .map(history -> history.getRecordForNamespaceAndUseCase(namespaceAndUseCase))
                .collect(Collectors.toList());
    }

    private List<ConsolidatedLearnerAndAcceptorRecord> combineLocalAndRemoteHistoryLogs(
            ConsolidatedLearnerAndAcceptorRecord consolidatedLocalRecord,
            List<ConsolidatedLearnerAndAcceptorRecord> consolidatedRemoteRecords) {
        return ImmutableList
                .<ConsolidatedLearnerAndAcceptorRecord>builder()
                .addAll(consolidatedRemoteRecords)
                .add(consolidatedLocalRecord)
                .build();
    }

    private ConsolidatedPaxosHistoryOnSingleNode buildRecordFromRemoteResponse(
            PaxosHistoryOnRemote historyOnRemote) {

        Map<NamespaceAndUseCase, List<PaxosLogWithAcceptedAndLearnedValues>> namespaceWisePaxosLogs
                = historyOnRemote.getLogs()
                .stream()
                .collect(Collectors.toMap(
                        LogsForNamespaceAndUseCase::getNamespaceAndUseCase,
                        LogsForNamespaceAndUseCase::getLogs)
                );

        return ConsolidatedPaxosHistoryOnSingleNode.of(KeyedStream
                .stream(namespaceWisePaxosLogs)
                .map(this::getConsolidatedLearnerAndAcceptorRecordFromRemotePaxosLogs)
                .collectToMap());
    }

    private ConsolidatedLearnerAndAcceptorRecord getConsolidatedLearnerAndAcceptorRecordFromRemotePaxosLogs(
            NamespaceAndUseCase unused,
            List<PaxosLogWithAcceptedAndLearnedValues> remoteLogs) {
        return ConsolidatedLearnerAndAcceptorRecord
                .of(getSequenceWiseLearnedAndAcceptedValuesFromRemoteLogs(remoteLogs));
    }

    private Map<Long, LearnedAndAcceptedValue> getSequenceWiseLearnedAndAcceptedValuesFromRemoteLogs(
            List<PaxosLogWithAcceptedAndLearnedValues> remoteLogs) {
        return remoteLogs
                .stream()
                .collect(Collectors.toMap(
                        PaxosLogWithAcceptedAndLearnedValues::getSeq,
                        remoteLog -> ImmutableLearnedAndAcceptedValue.of(
                                remoteLog.getPaxosValue(),
                                remoteLog.getAcceptedState())));
    }

    private PaxosHistoryOnRemote fetchHistoryFromRemote(List<HistoryQuery> historyQueries,
            TimeLockPaxosHistoryProvider remote) {
        try {
            return remote.getPaxosHistory(AUTH_HEADER, historyQueries);
        } catch (Exception exception) {
            log.warn("The remote failed to provide the history,"
                    + "we cannot perform corruption checks without history from all nodes.", exception);
            throw exception;
        }
    }

    private Map.Entry<NamespaceAndUseCase, HistoryQuery> buildHistoryQuery(
            NamespaceAndUseCase namespaceAndUseCase, Long seq) {
        return Maps.immutableEntry(namespaceAndUseCase, HistoryQuery.of(namespaceAndUseCase, seq));
    }
}
