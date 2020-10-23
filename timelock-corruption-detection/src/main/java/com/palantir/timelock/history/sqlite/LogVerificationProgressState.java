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
import com.palantir.timelock.history.mappers.ProgressComponentMapper;
import com.palantir.timelock.history.models.ProgressComponents;
import java.util.Optional;
import java.util.function.Function;
import javax.sql.DataSource;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.immutables.JdbiImmutables;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindPojo;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

public final class LogVerificationProgressState {
    private static final long INITIAL_PROGRESS = -1L;

    private final Jdbi jdbi;

    private LogVerificationProgressState(Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    public static LogVerificationProgressState create(DataSource dataSource) {
        Jdbi jdbi = Jdbi.create(dataSource).installPlugin(new SqlObjectPlugin());
        jdbi.getConfig(JdbiImmutables.class).registerImmutable(Client.class).registerImmutable(ProgressComponents.class);
        jdbi.registerRowMapper(new ProgressComponentMapper());
        LogVerificationProgressState state = new LogVerificationProgressState(jdbi);
        state.initialize();
        return state;
    }

    private void initialize() {
        execute(LogVerificationProgressState.Queries::createVerificationProgressStateTable);
    }


    public ProgressComponents resetProgressState(Client client, String useCase, long greatestLogSeq) {
        return execute(dao -> {
            dao.updateProgressStateAndProgressLimit(client, useCase, INITIAL_PROGRESS, greatestLogSeq);
            return ProgressComponents.builder().progressState(INITIAL_PROGRESS).progressLimit(greatestLogSeq).build();
        });
    }

    public void updateProgress(Client client, String useCase, long seq) {
        execute(dao -> {
            dao.updateProgress(client, useCase, seq);
            return null;
        });
    }

    public Optional<ProgressComponents> getProgressComponents(Client client, String useCase) {
        return execute(dao -> dao.getProgressComponents(client, useCase));
    }

    private <T> T execute(Function<LogVerificationProgressState.Queries, T> call) {
        return jdbi.withExtension(LogVerificationProgressState.Queries.class, call::apply);
    }

    public interface Queries {
        @SqlUpdate("CREATE TABLE IF NOT EXISTS logVerificationProgress (namespace TEXT, useCase TEXT, seq BIGINT, "
                + "progressLimit BIGINT, "
                + "PRIMARY KEY(namespace, useCase))")
        boolean createVerificationProgressStateTable();

        @SqlUpdate("INSERT OR REPLACE INTO logVerificationProgress (namespace, useCase, seq, progressLimit) VALUES"
                + " (:namespace.value, :useCase, :seq, :progressLimit)")
        boolean updateProgressStateAndProgressLimit(
                @BindPojo("namespace") Client namespace, @Bind("useCase") String useCase,
                @Bind("seq") long seq, @Bind("progressLimit") long progressLimit);

        @SqlUpdate("UPDATE logVerificationProgress SET seq = :seq "
                + "WHERE namespace = :namespace.value AND useCase = :useCase")
        boolean updateProgress(
                @BindPojo("namespace") Client namespace, @Bind("useCase") String useCase, @Bind("seq") long seq);

        @SqlQuery("SELECT seq, progressLimit FROM logVerificationProgress "
                + "WHERE namespace = :namespace.value AND useCase = :useCase")
        Optional<ProgressComponents> getProgressComponents(@BindPojo("namespace") Client namespace,
                @Bind("useCase") String useCase);
    }
}
