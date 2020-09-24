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

import com.palantir.paxos.Client;

public final class LogVerificationProgressState {
    private final Jdbi jdbi;

    private LogVerificationProgressState(Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    public static LogVerificationProgressState create(DataSource dataSource) {
        Jdbi jdbi = Jdbi.create(dataSource).installPlugin(new SqlObjectPlugin());
        jdbi.getConfig(JdbiImmutables.class).registerImmutable(Client.class);
        LogVerificationProgressState state = new LogVerificationProgressState(jdbi);
        state.initialize();
        return state;
    }

    private void initialize() {
        execute(LogVerificationProgressState.Queries::createVerificationProgressStateTable);
    }

    public boolean updateProgress(Client client, String useCase, long seq) {
        return execute(dao -> dao.updateProgress(client, useCase, seq));
    }

    public Optional<Long> getLastVerifiedSeq(Client client, String useCase) {
        return execute(dao -> dao.getLastVerifiedSeq(client, useCase));
    }

    private <T> T execute(Function<LogVerificationProgressState.Queries, T> call) {
        return jdbi.withExtension(LogVerificationProgressState.Queries.class, call::apply);
    }

    public interface Queries {
        @SqlUpdate("CREATE TABLE IF NOT EXISTS log_verification_progress (namespace TEXT, useCase TEXT, seq BIGINT,"
                + "PRIMARY KEY(namespace, useCase))")
        boolean createVerificationProgressStateTable();

        @SqlUpdate("INSERT OR REPLACE INTO log_verification_progress (namespace, useCase, seq) VALUES"
                + " (:namespace.value, :useCase, :seq)")
        boolean updateProgress(
                @BindPojo("namespace") Client namespace,
                @Bind("useCase") String useCase,
                @Bind("version") long version);

        @SqlQuery("SELECT version FROM log_verification_progress "
                + "WHERE namespace = :namespace.value AND useCase = :useCase")
        Optional<Long> getLastVerifiedSeq(@BindPojo("namespace") Client namespace, @Bind("useCase") String useCase);
    }
}
