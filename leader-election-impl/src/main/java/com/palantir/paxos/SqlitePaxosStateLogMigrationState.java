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
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.immutables.JdbiImmutables;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindPojo;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

public final class SqlitePaxosStateLogMigrationState {
    private final Client namespace;
    private final String useCase;
    private final Jdbi jdbi;

    private SqlitePaxosStateLogMigrationState(NamespaceAndUseCase namespaceAndUseCase, Jdbi jdbi) {
        this.namespace = namespaceAndUseCase.namespace();
        this.useCase = namespaceAndUseCase.useCase();
        this.jdbi = jdbi;
    }

    public static SqlitePaxosStateLogMigrationState create(NamespaceAndUseCase namespaceAndUseCase,
            Supplier<Connection> connectionSupplier) {
        Jdbi jdbi = Jdbi.create(connectionSupplier::get).installPlugin(new SqlObjectPlugin());
        jdbi.getConfig(JdbiImmutables.class).registerImmutable(Client.class);
        SqlitePaxosStateLogMigrationState state = new SqlitePaxosStateLogMigrationState(namespaceAndUseCase, jdbi);
        state.initialize();
        return state;
    }

    private void initialize() {
        execute(Queries::createTable);
    }

    public void migrateToValidationState() {
        execute(dao -> dao.migrateToVersion(namespace, useCase, States.VALIDATION.schemaVersion));
    }

    public void migrateToMigratedState() {
        execute(dao -> dao.migrateToVersion(namespace, useCase, States.MIGRATED.schemaVersion));
    }

    public boolean hasMigratedFromInitialState() {
        return !Objects.equals(States.NONE.getSchemaVersion(), execute(dao -> dao.getVersion(namespace, useCase)));
    }

    public boolean isInValidationState() {
        return States.VALIDATION.getSchemaVersion().equals(execute(dao -> dao.getVersion(namespace, useCase)));
    }

    public boolean isInMigratedState() {
        return States.MIGRATED.getSchemaVersion().equals(execute(dao -> dao.getVersion(namespace, useCase)));
    }

    private <T> T execute(Function<Queries, T> call) {
        return jdbi.withExtension(Queries.class, call::apply);
    }

    public interface Queries {
        @SqlUpdate("CREATE TABLE IF NOT EXISTS migration_state (namespace TEXT, useCase TEXT, version INT,"
                + "PRIMARY KEY(namespace, useCase))")
        boolean createTable();

        @SqlUpdate("INSERT OR REPLACE INTO migration_state (namespace, useCase, version) VALUES"
                + " (:namespace.value, :useCase, :version)")
        boolean migrateToVersion(
                @BindPojo("namespace") Client namespace,
                @Bind("useCase") String useCase,
                @Bind("version") int version);

        @SqlQuery("SELECT version FROM migration_state WHERE namespace = :namespace.value AND useCase = :useCase")
        Integer getVersion(@BindPojo("namespace") Client namespace, @Bind("useCase") String useCase);
    }

    private enum States {
        NONE(null), VALIDATION(0), MIGRATED(1);

        Integer schemaVersion;
        States(Integer schemaVersion) {
            this.schemaVersion = schemaVersion;
        }

        Integer getSchemaVersion() {
            return schemaVersion;
        }
    }
}
