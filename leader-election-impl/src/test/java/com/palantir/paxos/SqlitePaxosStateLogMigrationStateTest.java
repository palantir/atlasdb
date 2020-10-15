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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import javax.sql.DataSource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SqlitePaxosStateLogMigrationStateTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final NamespaceAndUseCase NAMESPACE_AND_USE_CASE = ImmutableNamespaceAndUseCase
            .of(Client.of("namespace"), "useCase");

    private DataSource dataSource;
    private SqlitePaxosStateLogMigrationState migrationState;

    @Before
    public void setup() {
        dataSource = SqliteConnections
                .getPooledDataSource(tempFolder.getRoot().toPath());
        migrationState = SqlitePaxosStateLogMigrationState.create(NAMESPACE_AND_USE_CASE, dataSource);
    }

    @Test
    public void initialStateTest() {
        assertThat(migrationState.hasMigratedFromInitialState()).isFalse();
        assertThat(migrationState.isInValidationState()).isFalse();
        assertThat(migrationState.isInMigratedState()).isFalse();
    }

    @Test
    public void canSetStateToValidation() {
        migrationState.migrateToValidationState();
        assertThat(migrationState.hasMigratedFromInitialState()).isTrue();
        assertThat(migrationState.isInValidationState()).isTrue();
        assertThat(migrationState.isInMigratedState()).isFalse();
    }

    @Test
    public void canSetStateToMigrated() {
        migrationState.migrateToMigratedState();
        assertThat(migrationState.hasMigratedFromInitialState()).isTrue();
        assertThat(migrationState.isInValidationState()).isFalse();
        assertThat(migrationState.isInMigratedState()).isTrue();
    }

    @Test
    public void canProgressThroughAndRepeatMigrationStates() {
        migrationState.migrateToValidationState();
        migrationState.migrateToMigratedState();
        migrationState.migrateToMigratedState();
        assertThat(migrationState.hasMigratedFromInitialState()).isTrue();
        assertThat(migrationState.isInValidationState()).isFalse();
        assertThat(migrationState.isInMigratedState()).isTrue();
    }

    @Test
    public void cannotRegressToLowerMigrationState() {
        migrationState.migrateToValidationState();
        migrationState.migrateToMigratedState();
        assertThatThrownBy(migrationState::migrateToValidationState).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void finishingMigrationForOneNamespaceDoesNotSetFlagForOthers() {
        migrationState.migrateToValidationState();

        SqlitePaxosStateLogMigrationState otherState = SqlitePaxosStateLogMigrationState.create(
                ImmutableNamespaceAndUseCase.of(Client.of("other"), "useCase"), dataSource);
        assertThat(otherState.hasMigratedFromInitialState()).isFalse();
        otherState.migrateToValidationState();
        assertThat(otherState.hasMigratedFromInitialState()).isTrue();
    }

    @Test
    public void finishingMigrationForOneUseCaseDoesNotSetFlagForOthers() {
        migrationState.migrateToValidationState();

        SqlitePaxosStateLogMigrationState otherState = SqlitePaxosStateLogMigrationState.create(
                ImmutableNamespaceAndUseCase.of(Client.of("namespace"), "other"), dataSource);
        assertThat(otherState.hasMigratedFromInitialState()).isFalse();
        otherState.migrateToValidationState();
        assertThat(otherState.hasMigratedFromInitialState()).isTrue();
    }

    @Test
    public void defaultCutoffIsNoLogEntry() {
        assertThat(migrationState.getCutoff()).isEqualTo(PaxosAcceptor.NO_LOG_ENTRY);
    }

    @Test
    public void canSetCutoff() {
        long expectedCutoff = 100L;
        migrationState.setCutoff(expectedCutoff);
        assertThat(migrationState.getCutoff()).isEqualTo(expectedCutoff);
    }
}
