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

package com.palantir.paxos;

import java.sql.Connection;
import java.util.function.Function;
import java.util.function.Supplier;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

public class SqlitePaxosStateLogMigrationState {
    private final String namespace;
    private final Jdbi jdbi;

    private SqlitePaxosStateLogMigrationState(String namespace, Jdbi jdbi) {
        this.namespace = namespace;
        this.jdbi = jdbi;
    }

    public static SqlitePaxosStateLogMigrationState create(String namespace, Supplier<Connection> connectionSupplier) {
        Jdbi jdbi = Jdbi.create(connectionSupplier::get).installPlugin(new SqlObjectPlugin());
        SqlitePaxosStateLogMigrationState state = new SqlitePaxosStateLogMigrationState(namespace, jdbi);
        state.initialize();
        return state;
    }

    private void initialize() {
        execute(Queries::createTable);
    }

    public void finishMigration() {
        execute(dao -> dao.finishMigration(namespace));
    }

    public boolean hasAlreadyMigrated() {
        return execute(dao -> dao.hasFinishedMigrating(namespace));
    }

    private <T> T execute(Function<Queries, T> call) {
        return jdbi.withExtension(Queries.class, call::apply);
    }

    public interface Queries {
        @SqlUpdate("CREATE TABLE IF NOT EXISTS migration_state (namespace TEXT PRIMARY KEY, version INT)")
        boolean createTable();

        @SqlUpdate("INSERT OR REPLACE INTO migration_state (namespace, version) VALUES (?, 0)")
        boolean finishMigration(String namespace);

        @SqlQuery("SELECT EXISTS (SELECT 1 FROM migration_state WHERE namespace = ?)")
        boolean hasFinishedMigrating(String namespace);
    }
}
