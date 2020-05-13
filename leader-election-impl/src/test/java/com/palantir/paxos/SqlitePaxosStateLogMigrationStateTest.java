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

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SqlitePaxosStateLogMigrationStateTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final NamespaceAndUseCase NAMESPACE_AND_USE_CASE = ImmutableNamespaceAndUseCase
            .of(Client.of("namespace"), "useCase");

    private Supplier<Connection> connSupplier;
    private SqlitePaxosStateLogMigrationState migrationState;

    @Before
    public void setup() {
        connSupplier = SqliteConnections
                .createDefaultNamedSqliteDatabaseAtPath(tempFolder.getRoot().toPath());
        migrationState = SqlitePaxosStateLogMigrationState.create(NAMESPACE_AND_USE_CASE, connSupplier);
    }

    @Test
    public void initialStateIsNotMigrated() {
        assertThat(migrationState.hasMigratedFromInitialState()).isFalse();
        assertThat(migrationState.isInValidationState()).isFalse();
    }

    @Test
    public void canSetStateToMigrated() {
        migrationState.migrateToValidationState();
        assertThat(migrationState.hasMigratedFromInitialState()).isTrue();
        assertThat(migrationState.isInValidationState()).isTrue();
    }

    @Test
    public void canSetStateToMigratedMultipleTimes() {
        migrationState.migrateToValidationState();
        migrationState.migrateToValidationState();
        migrationState.migrateToValidationState();
        assertThat(migrationState.hasMigratedFromInitialState()).isTrue();
        assertThat(migrationState.isInValidationState()).isTrue();
    }

    @Test
    public void finishingMigrationForOneNamespaceDoesNotSetFlagForOthers() {
        migrationState.migrateToValidationState();

        SqlitePaxosStateLogMigrationState otherState = SqlitePaxosStateLogMigrationState
                .create(ImmutableNamespaceAndUseCase.of(Client.of("other"), "useCase"), connSupplier);
        assertThat(otherState.hasMigratedFromInitialState()).isFalse();
        otherState.migrateToValidationState();
        assertThat(otherState.hasMigratedFromInitialState()).isTrue();
    }

    @Test
    public void finishingMigrationForOneUseCaseDoesNotSetFlagForOthers() {
        migrationState.migrateToValidationState();

        SqlitePaxosStateLogMigrationState otherState = SqlitePaxosStateLogMigrationState
                .create(ImmutableNamespaceAndUseCase.of(Client.of("namespace"), "other"), connSupplier);
        assertThat(otherState.hasMigratedFromInitialState()).isFalse();
        otherState.migrateToValidationState();
        assertThat(otherState.hasMigratedFromInitialState()).isTrue();
    }
}
