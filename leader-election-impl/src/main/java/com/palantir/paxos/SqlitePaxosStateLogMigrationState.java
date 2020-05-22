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
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.immutables.JdbiImmutables;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindPojo;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;

public final class SqlitePaxosStateLogMigrationState {
    private final Client namespace;
    private final String useCase;
    private final Jdbi jdbi;
    private final ReadWriteLock sharedLock;

    private SqlitePaxosStateLogMigrationState(NamespaceAndUseCase namespaceAndUseCase, Jdbi jdbi,
            ReadWriteLock sharedLock) {
        this.namespace = namespaceAndUseCase.namespace();
        this.useCase = namespaceAndUseCase.useCase();
        this.jdbi = jdbi;
        this.sharedLock = sharedLock;
    }

    static SqlitePaxosStateLogMigrationState create(NamespaceAndUseCase namespaceAndUseCase,
            Supplier<Connection> connectionSupplier, ReadWriteLock sharedLock) {
        Jdbi jdbi = Jdbi.create(connectionSupplier::get).installPlugin(new SqlObjectPlugin());
        jdbi.getConfig(JdbiImmutables.class).registerImmutable(Client.class);
        SqlitePaxosStateLogMigrationState state = new SqlitePaxosStateLogMigrationState(namespaceAndUseCase, jdbi,
                sharedLock);
        state.initialize();
        return state;
    }

    private void initialize() {
        executeWrite(Queries::createMigrationStateTable);
        executeWrite(Queries::createMigrationCutoffTable);
    }

    public void migrateToValidationState() {
        executeWrite(migrateToState(States.VALIDATION));
    }

    public void migrateToMigratedState() {
        executeWrite(migrateToState(States.MIGRATED));
    }

    public boolean hasMigratedFromInitialState() {
        return executeRead(dao -> dao.getVersion(namespace, useCase).isPresent());
    }

    public boolean isInValidationState() {
        return executeRead(dao -> dao.getVersion(namespace, useCase)
                .map(States.VALIDATION.getSchemaVersion()::equals)
                .orElse(false));
    }

    public boolean isInMigratedState() {
        return executeRead(dao -> dao.getVersion(namespace, useCase)
                .map(States.MIGRATED.getSchemaVersion()::equals)
                .orElse(false));
    }

    public void setCutoff(long value) {
        executeWrite(dao -> dao.setCutoff(namespace, useCase, value));
    }

    public long getCutoff() {
        return executeRead(dao -> dao.getCutoff(namespace, useCase)).orElse(Long.MIN_VALUE);
    }

    private <T> T executeWrite(Function<Queries, T> call) {
        sharedLock.writeLock().lock();
        try {
            return execute(call);
        } finally {
            sharedLock.writeLock().unlock();
        }
    }

    private <T> T executeRead(Function<Queries, T> call) {
        sharedLock.readLock().lock();
        try {
            return execute(call);
        } finally {
            sharedLock.readLock().unlock();
        }
    }

    private <T> T execute(Function<Queries, T> call) {
        return jdbi.withExtension(Queries.class, call::apply);
    }

    private Function<Queries, Boolean> migrateToState(States state) {
        return dao -> {
            assertCurrentStateAtMost(dao, state);
            return dao.migrateToVersion(namespace, useCase, state.getSchemaVersion());
        };
    }

    private void assertCurrentStateAtMost(Queries dao, States state) {
        dao.getVersion(namespace, useCase).ifPresent(currentVersion ->
                Preconditions.checkState(currentVersion <= state.getSchemaVersion(),
                        "Could not update migration state because it would cause us to go back in state version.",
                        SafeArg.of("currentVersion", currentVersion),
                        SafeArg.of("migrationState", state),
                        SafeArg.of("migrationVersion", state.getSchemaVersion())));
    }

    public interface Queries {
        @SqlUpdate("CREATE TABLE IF NOT EXISTS migration_state (namespace TEXT, useCase TEXT, version INT,"
                + "PRIMARY KEY(namespace, useCase))")
        boolean createMigrationStateTable();

        @SqlUpdate("CREATE TABLE IF NOT EXISTS migration_cutoff (namespace TEXT, useCase TEXT, cutoff BIGINT,"
                + "PRIMARY KEY(namespace, useCase))")
        boolean createMigrationCutoffTable();

        @SqlUpdate("INSERT OR REPLACE INTO migration_state (namespace, useCase, version) VALUES"
                + " (:namespace.value, :useCase, :version)")
        boolean migrateToVersion(
                @BindPojo("namespace") Client namespace,
                @Bind("useCase") String useCase,
                @Bind("version") int version);

        @SqlUpdate("INSERT OR REPLACE INTO migration_cutoff (namespace, useCase, cutoff) VALUES"
                + " (:namespace.value, :useCase, :cutoff)")
        boolean setCutoff(
                @BindPojo("namespace") Client namespace,
                @Bind("useCase") String useCase,
                @Bind("cutoff") long cutoff);

        @SqlQuery("SELECT version FROM migration_state WHERE namespace = :namespace.value AND useCase = :useCase")
        Optional<Integer> getVersion(@BindPojo("namespace") Client namespace, @Bind("useCase") String useCase);

        @SqlQuery("SELECT cutoff FROM migration_cutoff WHERE namespace = :namespace.value AND useCase = :useCase")
        Optional<Long> getCutoff(@BindPojo("namespace") Client namespace, @Bind("useCase") String useCase);
    }

    private enum States {
        NONE(null), VALIDATION(0), MIGRATED(1);

        private final Integer schemaVersion;

        States(Integer schemaVersion) {
            this.schemaVersion = schemaVersion;
        }

        Integer getSchemaVersion() {
            return schemaVersion;
        }
    }
}
