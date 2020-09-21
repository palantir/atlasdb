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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import com.palantir.history.sqlite.LocalHistoryLoader;
import com.palantir.history.sqlite.LogVerificationProgressState;
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.paxos.SqlitePaxosStateLogQueries;

public class PaxosLogHistoryProvider {
    private final DataSource dataSource;
    private final LogVerificationProgressState logVerificationProgressState;
    private final LocalHistoryLoader localHistoryLoader;
    private final Jdbi jdbi;
    private Map<NamespaceAndUseCase, Long> verificationProgressState = new ConcurrentHashMap<>();

    private static final long INITIAL_PROGRESS = -1L;

    public PaxosLogHistoryProvider(DataSource dataSource) {
        Jdbi jdbi = Jdbi.create(dataSource).installPlugin(new SqlObjectPlugin());
        init(jdbi);
        this.jdbi = jdbi;
        this.logVerificationProgressState = LogVerificationProgressState.create(dataSource);
        this.localHistoryLoader = LocalHistoryLoader.create(dataSource);
        this.dataSource = dataSource;
    }

    //todo revisit
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
        logVerificationProgressState.updateProgress(namespaceAndUseCase.namespace(), namespaceAndUseCase.useCase(),
                INITIAL_PROGRESS);
        return INITIAL_PROGRESS;
    }
}
