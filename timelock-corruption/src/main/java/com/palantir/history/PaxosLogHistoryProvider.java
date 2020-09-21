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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import com.google.common.collect.ImmutableList;
import com.palantir.common.streams.KeyedStream;
import com.palantir.history.models.CompletePaxosHistoryForNamespaceAndUseCase;
import com.palantir.history.models.PaxosHistoryOnSingleNode;
import com.palantir.history.sqlite.LogVerificationProgressState;
import com.palantir.history.sqlite.SqlitePaxosStateLogHistory;
import com.palantir.history.util.UseCaseUtils;
import com.palantir.paxos.ImmutableNamespaceAndUseCase;
import com.palantir.paxos.NamespaceAndUseCase;

public class PaxosLogHistoryProvider {
    private final LogVerificationProgressState logVerificationProgressState;
    private final LocalHistoryLoader localHistoryLoader;
    private final SqlitePaxosStateLogHistory sqlitePaxosStateLogHistory;
    private Map<NamespaceAndUseCase, Long> verificationProgressStateCache = new ConcurrentHashMap<>();


    public PaxosLogHistoryProvider(DataSource dataSource) {
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

        // TBD get history from remotes, merge, return
        return ImmutableList.of();
    }
}
