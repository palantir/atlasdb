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

package com.palantir.history.sqlite;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import javax.sql.DataSource;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.immutables.JdbiImmutables;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindPojo;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

import com.palantir.history.mappers.AcceptorPaxosRoundMapper;
import com.palantir.history.mappers.LearnerPaxosRoundMapper;
import com.palantir.history.mappers.NamespaceAndUseCaseMapper;
import com.palantir.history.models.AcceptorUseCase;
import com.palantir.history.models.ImmutableLearnerAndAcceptorRecords;
import com.palantir.history.models.LearnerAndAcceptorRecords;
import com.palantir.history.models.LearnerUseCase;
import com.palantir.paxos.Client;
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.paxos.PaxosAcceptorState;
import com.palantir.paxos.PaxosRound;
import com.palantir.paxos.PaxosValue;
import com.palantir.paxos.SqlitePaxosStateLog;

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

    public LearnerAndAcceptorRecords getLearnerAndAcceptorLogsSince(
            Client namespace, LearnerUseCase learnerUseCase, AcceptorUseCase acceptorUseCase, long seq) {
        return execute(dao -> ImmutableLearnerAndAcceptorRecords.of(
                dao.getLearnerLogsSince(namespace, learnerUseCase.value(), seq),
                dao.getAcceptorLogsSince(namespace, acceptorUseCase.value(), seq)));
    }

    private <T> T execute(Function<Queries, T> call) {
        return jdbi.withExtension(Queries.class, call::apply);
    }

    public interface Queries {
        @SqlQuery("SELECT DISTINCT namespace, useCase FROM paxosLog")
        Set<NamespaceAndUseCase> getAllNamespaceAndUseCaseTuples();

//        TODO(snanda): For now, limit is based on approximation and has not been tested with remotes. We need to
//         revisit this once we have the remote history providers set up. Also, we may have to make it configurable to
//         accommodate the rate at which logs are being published.
        @SqlQuery("SELECT seq, val FROM paxosLog "
                + "WHERE namespace = :namespace.value AND useCase = :useCase AND seq > :seq "
                + "ORDER BY seq ASC LIMIT 500")
        Map<Long, PaxosValue> getLearnerLogsSince(
                @BindPojo("namespace") Client namespace,
                @Bind("useCase") String useCase,
                @Bind("seq") long seq);

        @SqlQuery("SELECT seq, val FROM paxosLog "
                + "WHERE namespace = :namespace.value AND useCase = :useCase AND seq > :seq "
                + "ORDER BY seq ASC LIMIT 500")
        Map<Long, PaxosAcceptorState> getAcceptorLogsSince(
                @BindPojo("namespace") Client namespace,
                @Bind("useCase") String useCase,
                @Bind("seq") long seq);
    }
}
