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

package com.palantir.timelock.history.sqlite;

import com.palantir.paxos.Client;
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosRound;
import com.palantir.paxos.PaxosValue;
import com.palantir.paxos.SqlitePaxosStateLog;
import com.palantir.timelock.history.HistoryQuerySequenceBounds;
import com.palantir.timelock.history.PaxosAcceptorData;
import com.palantir.timelock.history.mappers.AcceptorPaxosRoundMapper;
import com.palantir.timelock.history.mappers.LearnerPaxosRoundMapper;
import com.palantir.timelock.history.mappers.NamespaceAndUseCaseMapper;
import com.palantir.timelock.history.models.AcceptorUseCase;
import com.palantir.timelock.history.models.ImmutableLearnerAndAcceptorRecords;
import com.palantir.timelock.history.models.LearnerAndAcceptorRecords;
import com.palantir.timelock.history.models.LearnerUseCase;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.immutables.JdbiImmutables;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindPojo;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

public final class SqlitePaxosStateLogHistory {
    private final Jdbi jdbi;

    private SqlitePaxosStateLogHistory(Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    public static SqlitePaxosStateLogHistory create(DataSource dataSource) {
        Jdbi jdbi = Jdbi.create(dataSource).installPlugin(new SqlObjectPlugin());
        jdbi.withExtension(SqlitePaxosStateLog.Queries.class, SqlitePaxosStateLog.Queries::createTable);
        jdbi.getConfig(JdbiImmutables.class)
                .registerImmutable(Client.class, PaxosRound.class, NamespaceAndUseCase.class);
        jdbi.registerRowMapper(new AcceptorPaxosRoundMapper())
                .registerRowMapper(new LearnerPaxosRoundMapper())
                .registerRowMapper(new NamespaceAndUseCaseMapper());
        return new SqlitePaxosStateLogHistory(jdbi);
    }

    public Set<NamespaceAndUseCase> getAllNamespaceAndUseCaseTuples() {
        return execute(Queries::getAllNamespaceAndUseCaseTuples);
    }

    public LearnerAndAcceptorRecords getLearnerAndAcceptorLogsInRange(
            Client namespace,
            LearnerUseCase learnerUseCase,
            AcceptorUseCase acceptorUseCase,
            HistoryQuerySequenceBounds querySequenceBounds,
            Supplier<Long> greatestDeletedSeqSupplier) {
        return execute(dao -> {
            long lowerBoundInclusive =
                    Math.max(querySequenceBounds.getLowerBoundInclusive(), greatestDeletedSeqSupplier.get() + 1);
            return ImmutableLearnerAndAcceptorRecords.of(
                    dao.getLearnerLogsInRange(
                            namespace,
                            learnerUseCase.value(),
                            lowerBoundInclusive,
                            querySequenceBounds.getUpperBoundInclusive()),
                    dao.getAcceptorLogsInRange(
                            namespace,
                            acceptorUseCase.value(),
                            lowerBoundInclusive,
                            querySequenceBounds.getUpperBoundInclusive()),
                    greatestDeletedSeqSupplier.get());
        });
    }

    public Map<Long, PaxosValue> getLearnerLogsSince(
            Client namespace, LearnerUseCase learnerUseCase, long lowerBoundInclusive, int learnerLogBatchSizeLimit) {
        return execute(dao -> dao.getLearnerLogsSince(
                namespace, learnerUseCase.value(), lowerBoundInclusive, learnerLogBatchSizeLimit));
    }

    public long getGreatestLogEntry(Client client, LearnerUseCase useCase) {
        return executeSqlitePaxosStateLogQuery(dao -> dao.getGreatestLogEntry(client, useCase.value()))
                .orElse(PaxosAcceptor.NO_LOG_ENTRY);
    }

    private <T> T execute(Function<Queries, T> call) {
        return jdbi.withExtension(Queries.class, call::apply);
    }

    private <T> T executeSqlitePaxosStateLogQuery(Function<SqlitePaxosStateLog.Queries, T> call) {
        return jdbi.withExtension(SqlitePaxosStateLog.Queries.class, call::apply);
    }

    public interface Queries {
        @SqlQuery("SELECT DISTINCT namespace, useCase FROM paxosLog")
        Set<NamespaceAndUseCase> getAllNamespaceAndUseCaseTuples();

        @SqlQuery("SELECT seq, val FROM paxosLog WHERE namespace = :namespace.value AND useCase = :useCase AND seq >="
                + " :lowerBoundInclusive AND seq <= :upperBoundInclusive")
        Map<Long, PaxosValue> getLearnerLogsInRange(
                @BindPojo("namespace") Client namespace,
                @Bind("useCase") String useCase,
                @Bind("lowerBoundInclusive") long lowerBoundInclusive,
                @Bind("upperBoundInclusive") long upperBoundInclusive);

        @SqlQuery("SELECT seq, val FROM paxosLog WHERE namespace = :namespace.value AND useCase = :useCase AND seq >="
                + " :lowerBoundInclusive AND seq <= :upperBoundInclusive")
        Map<Long, PaxosAcceptorData> getAcceptorLogsInRange(
                @BindPojo("namespace") Client namespace,
                @Bind("useCase") String useCase,
                @Bind("lowerBoundInclusive") long lowerBoundInclusive,
                @Bind("upperBoundInclusive") long upperBoundInclusive);

        @SqlQuery("SELECT seq, val FROM paxosLog WHERE namespace = :namespace.value AND useCase = :useCase AND seq >="
                + " :lowerBoundInclusive ORDER BY seq ASC LIMIT :limit")
        Map<Long, PaxosValue> getLearnerLogsSince(
                @BindPojo("namespace") Client namespace,
                @Bind("useCase") String useCase,
                @Bind("lowerBoundInclusive") long lowerBoundInclusive,
                @Bind("limit") long learnerLogBatchSizeLimit);
    }
}
