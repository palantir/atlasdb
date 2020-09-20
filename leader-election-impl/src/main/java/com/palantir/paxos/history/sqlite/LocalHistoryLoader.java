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

package com.palantir.paxos.history.sqlite;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;

import javax.sql.DataSource;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.paxos.PaxosAcceptorState;
import com.palantir.paxos.PaxosValue;
import com.palantir.paxos.SqlitePaxosStateLogQueries;
import com.palantir.paxos.history.models.ImmutableLearnerAndAcceptorRecords;
import com.palantir.paxos.history.models.ImmutablePaxosHistoryOnSingleNode;
import com.palantir.paxos.history.models.LearnerAndAcceptorRecords;
import com.palantir.paxos.history.models.PaxosHistoryOnSingleNode;

//todo caching
public class LocalHistoryLoader {
    private final Jdbi jdbi;

    private LocalHistoryLoader(Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    public static LocalHistoryLoader create(DataSource dataSource) {
        Jdbi jdbi = Jdbi.create(dataSource).installPlugin(new SqlObjectPlugin());
        jdbi.withExtension(SqlitePaxosStateLogQueries.class, SqlitePaxosStateLogQueries::createTable);
        return new LocalHistoryLoader(jdbi);
    }

    // todo maybe refactor?
    public PaxosHistoryOnSingleNode getLocalPaxosHistory(
            Map<NamespaceAndUseCase, Long> laseVerifiedSeqNamespaceAndUseCaseWise) {
        return ImmutablePaxosHistoryOnSingleNode.of(KeyedStream.stream(laseVerifiedSeqNamespaceAndUseCaseWise)
                .map(this::loadLocalHistory)
                .collectToMap());
    }

    private LearnerAndAcceptorRecords loadLocalHistory(NamespaceAndUseCase namespaceAndUseCase, Long seq) {
        return ImmutableLearnerAndAcceptorRecords.of(getLearnerLogsForNamespaceAndUseCaseSince(namespaceAndUseCase, seq),
                getAcceptorLogsForNamespaceAndUseCaseSince(namespaceAndUseCase, seq));
    }

    private ConcurrentSkipListMap<Long, PaxosValue> getLearnerLogsForNamespaceAndUseCaseSince(
            NamespaceAndUseCase namespaceAndUseCase, long seq) {
        ConcurrentSkipListMap<Long, PaxosValue> map = new ConcurrentSkipListMap<>();
        execute(dao -> dao.getLearnerLogsSince(namespaceAndUseCase.namespace(), namespaceAndUseCase.useCase(), seq))
                .forEach(paxosRound -> map.put(paxosRound.sequence(), paxosRound.value()));
        return map;
    }

    // todo repetition?
    private ConcurrentSkipListMap<Long, PaxosAcceptorState> getAcceptorLogsForNamespaceAndUseCaseSince(
            NamespaceAndUseCase namespaceAndUseCase, long seq) {
        ConcurrentSkipListMap<Long, PaxosAcceptorState> map = new ConcurrentSkipListMap<>();
        execute(dao -> dao.getAcceptorLogsSince(
                namespaceAndUseCase.namespace(), namespaceAndUseCase.useCase(), seq))
                .forEach(paxosRound -> map.put(paxosRound.sequence(), paxosRound.value()));
        return map;
    }

    private <T> T execute(Function<SqlitePaxosStateLogQueries, T> call) {
        return jdbi.withExtension(SqlitePaxosStateLogQueries.class, call::apply);
    }
}
