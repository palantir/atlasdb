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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import javax.sql.DataSource;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.palantir.common.streams.KeyedStream;
import com.palantir.history.models.CompletePaxosHistoryForNamespaceAndUseCase;
import com.palantir.history.models.ImmutableCompletePaxosHistoryForNamespaceAndUseCase;
import com.palantir.history.models.ImmutableLearnedAndAcceptedValue;
import com.palantir.history.models.LearnedAndAcceptedValue;
import com.palantir.history.models.LearnerAndAcceptorRecords;
import com.palantir.history.models.PaxosHistoryOnSingleNode;
import com.palantir.history.sqlite.LogVerificationProgressState;
import com.palantir.history.sqlite.SqlitePaxosStateLogHistory;
import com.palantir.history.util.UseCaseUtils;
import com.palantir.paxos.ImmutableNamespaceAndUseCase;
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.paxos.PaxosAcceptorState;
import com.palantir.paxos.PaxosValue;
import com.palantir.timelock.history.HistoryQuery;
import com.palantir.timelock.history.LogsForNamespaceAndUseCase;
import com.palantir.timelock.history.PaxosLogWithAcceptedAndLearnedValues;
import com.palantir.timelock.history.TimeLockPaxosHistoryProvider;
import com.palantir.tokens.auth.AuthHeader;

public class PaxosLogHistoryProvider {
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

    public List<CompletePaxosHistoryForNamespaceAndUseCase> getHistory() {
        Map<NamespaceAndUseCase, Long> lastVerifiedSequences = KeyedStream
                .of(getNamespaceAndUseCaseTuples().stream())
                .map(namespaceAndUseCase -> verificationProgressStateCache.computeIfAbsent(namespaceAndUseCase,
                        this::getOrInsertVerificationState))
                .collectToMap();

        PaxosHistoryOnSingleNode localPaxosHistory = localHistoryLoader.getLocalPaxosHistory(
                lastVerifiedSequences);

        List<HistoryQuery> historyQueries = KeyedStream.stream(lastVerifiedSequences)
                .mapEntries(this::getQueriesForRemote)
                .values()
                .collect(Collectors.toList());


        List<List<LogsForNamespaceAndUseCase>> rawHistoryFromAllRemotes = remoteHistoryProviders.stream().map(
                remote -> fetchHistoryFromRemote(historyQueries, remote)).collect(Collectors.toList());

        List<Map<NamespaceAndUseCase, Map<Long, LearnedAndAcceptedValue>>> ahhh = rawHistoryFromAllRemotes.stream().map(
                this::buildHistoryOnSingleNode).collect(Collectors.toList());

        return lastVerifiedSequences.keySet().stream().map(this::buildCompleteHistory).collect(Collectors.toList());
    }

    private CompletePaxosHistoryForNamespaceAndUseCase buildCompleteHistory(NamespaceAndUseCase namespaceAndUseCase,
            LearnerAndAcceptorRecords records, List<Map<Long, LearnedAndAcceptedValue>> remoteRecords) {
        Map<Long, PaxosValue> learnerRecords = records.learnerRecords();
        Map<Long, PaxosAcceptorState> acceptorRecords = records.acceptorRecords();

        long minSeq = Math.min(Collections.min(learnerRecords.keySet()), Collections.min(acceptorRecords.keySet()));

        long maxSeq = Math.max(Collections.max(learnerRecords.keySet()), Collections.max(acceptorRecords.keySet()));

        Map<Long, LearnedAndAcceptedValue> local = LongStream.rangeClosed(minSeq, maxSeq).boxed()
                .collect(Collectors.toMap(Function.identity(),
                        seq -> ImmutableLearnedAndAcceptedValue.of(records.getLearnedValueAtSeqIfExists((Long) seq),
                                records.getAcceptedValueAtSeqIfExists((Long) seq))));

        ImmutableList<Map<Long, LearnedAndAcceptedValue>> list
                = ImmutableList.<Map<Long, LearnedAndAcceptedValue>>builder().addAll(remoteRecords).add(local).build();
        return ImmutableCompletePaxosHistoryForNamespaceAndUseCase.of(namespaceAndUseCase.namespace(),
                namespaceAndUseCase.useCase(), list);
    }

    private Map<NamespaceAndUseCase, Map<Long, LearnedAndAcceptedValue>> buildHistoryOnSingleNode(
            List<LogsForNamespaceAndUseCase> logsForNamespaceAndUseCases) {
        Map<NamespaceAndUseCase, List<PaxosLogWithAcceptedAndLearnedValues>> temp
                = logsForNamespaceAndUseCases
                .stream()
                .collect(
                        Collectors.toMap(LogsForNamespaceAndUseCase::getNamespaceAndUseCase,
                                LogsForNamespaceAndUseCase::getLogs));

        return KeyedStream.stream(temp).map(this::mapLogsAgainstSequenceNumbers).collectToMap();
    }

    private Map<Long, LearnedAndAcceptedValue> mapLogsAgainstSequenceNumbers(NamespaceAndUseCase namespaceAndUseCase,
            List<PaxosLogWithAcceptedAndLearnedValues> logs) {
        return logs.stream().collect(Collectors.toMap(PaxosLogWithAcceptedAndLearnedValues::getSeq,
                        log -> ImmutableLearnedAndAcceptedValue.of(log.getPaxosValue(), log.getAcceptedState())));
    }

    public List<LogsForNamespaceAndUseCase> fetchHistoryFromRemote(List<HistoryQuery> historyQueries,
            TimeLockPaxosHistoryProvider remote) {
        try {
            return remote.getPaxosHistory(AUTH_HEADER, historyQueries);
        } catch (Exception e) {
            // todo ????
            return ImmutableList.of();
        }
    }

    private Map.Entry<NamespaceAndUseCase, HistoryQuery> getQueriesForRemote(
            NamespaceAndUseCase namespaceAndUseCase, Long seq) {
        return Maps.immutableEntry(namespaceAndUseCase, HistoryQuery.of(namespaceAndUseCase, seq));
    }
}
