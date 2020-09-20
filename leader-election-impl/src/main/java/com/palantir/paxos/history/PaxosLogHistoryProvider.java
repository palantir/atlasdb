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

package com.palantir.paxos.history;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import com.google.common.collect.ImmutableList;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.paxos.PaxosAcceptorState;
import com.palantir.paxos.PaxosValue;
import com.palantir.paxos.SqlitePaxosStateLogQueries;
import com.palantir.paxos.history.models.CompletePaxosHistoryForNamespaceAndUsecase;
import com.palantir.paxos.history.models.ImmutableCompletePaxosHistoryForNamespaceAndUsecase;
import com.palantir.paxos.history.sqlite.LocalHistoryLoader;
import com.palantir.paxos.history.sqlite.LogVerificationProgressState;

public class PaxosLogHistoryProvider {
    private final DataSource dataSource;
    private final LogVerificationProgressState logVerificationProgressState;
    private final LocalHistoryLoader localHistoryLoader;
    private final Jdbi jdbi;
    private Map<NamespaceAndUseCase, Long> verificationProgressState = new ConcurrentHashMap<>();

    public PaxosLogHistoryProvider(DataSource dataSource) {
        Jdbi jdbi = Jdbi.create(dataSource).installPlugin(new SqlObjectPlugin());
//        init(jdbi);
        this.jdbi = jdbi;
        this.logVerificationProgressState = LogVerificationProgressState.create(dataSource);
        this.localHistoryLoader = LocalHistoryLoader.create(dataSource);
        this.dataSource = dataSource;
    }

    //todo revisit: fill her up upon start up
    public void init(Jdbi jdbi) {
        jdbi.withExtension(SqlitePaxosStateLogQueries.class, SqlitePaxosStateLogQueries::createTable);
        jdbi.withExtension(SqlitePaxosStateLogQueries.class,
                SqlitePaxosStateLogQueries::getAllNamespaceAndUseCaseTuples)
                .forEach(namespaceAndUseCase -> verificationProgressState.computeIfAbsent(namespaceAndUseCase,
                        this::getOrInsertVerificationState));
    }

    private Long getOrInsertVerificationState(NamespaceAndUseCase namespaceAndUseCase) {
        Optional<Long> lastVerifiedSeq = logVerificationProgressState.getLastVerifiedSeq(
                namespaceAndUseCase.namespace(), namespaceAndUseCase.useCase());
        return lastVerifiedSeq.orElseGet(() -> insertVerificationState(namespaceAndUseCase));
    }

    private Long insertVerificationState(NamespaceAndUseCase namespaceAndUseCase) {
        logVerificationProgressState.updateProgress(namespaceAndUseCase.namespace(), namespaceAndUseCase.useCase(), -1L);
        return -1L;
    }

    public List<CompletePaxosHistoryForNamespaceAndUsecase> paxosHistory(
            Map<NamespaceAndUseCase, Long> laseVerifiedSeqNamespaceAndUseCaseWise) {
        return KeyedStream.stream(laseVerifiedSeqNamespaceAndUseCaseWise)
                .map(this::mapToRecord)
                .values()
                .collect(Collectors.toList());
    }

    private CompletePaxosHistoryForNamespaceAndUsecase mapToRecord(
            NamespaceAndUseCase namespaceAndUseCase, long seq) {
        List<ConcurrentSkipListMap<Long, PaxosValue>> learnerRecord
                = fetchLearnerRecordsForNamespaceAndUseCase(namespaceAndUseCase, seq);
        List<ConcurrentSkipListMap<Long, PaxosAcceptorState>> acceptorRecord
                = fetchAcceptorRecordsForNamespaceAndUseCase(namespaceAndUseCase, seq);
        return ImmutableCompletePaxosHistoryForNamespaceAndUsecase.of(namespaceAndUseCase.namespace(),
                namespaceAndUseCase.useCase(),
                learnerRecord,
                acceptorRecord);
    }

    private List<ConcurrentSkipListMap<Long, PaxosValue>> fetchLearnerRecordsForNamespaceAndUseCase(
            NamespaceAndUseCase namespaceAndUseCase, long seq) {
        // todo remotes pending
        return ImmutableList.of(localHistoryLoader.getLearnerLogsForNamespaceAndUseCaseSince(namespaceAndUseCase, seq));
    }

    private List<ConcurrentSkipListMap<Long, PaxosAcceptorState>> fetchAcceptorRecordsForNamespaceAndUseCase(
            NamespaceAndUseCase namespaceAndUseCase, long seq) {
        // todo remotes pending
        return ImmutableList.of(localHistoryLoader.getAcceptorLogsForNamespaceAndUseCaseSince(namespaceAndUseCase, seq));
    }

}
