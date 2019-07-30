/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.migration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static com.palantir.atlasdb.migration.MigrationCoordinationServiceImpl.DEFAULT_TABLE_MIGRATION_STATE;

import java.util.Optional;

import org.junit.Test;

import com.palantir.atlasdb.coordination.CoordinationServiceImpl;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.keyvalue.api.TableReference;

@SuppressWarnings("unchecked") // Mocks of generic types
public class MigrationCoordinationServiceImplTest {
    private static final TableReference TABLE = TableReference.fromString("table.some");
    private static final TableReference TARGET_TABLE = TableReference.fromString("table.target");
    private static final TableReference OTHER_TABLE = TableReference.fromString("table.other");
    private static final MigrationState STATE = MigrationState.WRITE_FIRST_ONLY;
    private static final MigrationState OTHER_STATE = MigrationState.WRITE_BOTH_READ_FIRST;
    private static final TableMigrationStateMap STATE_MAP = TableMigrationStateMap.builder()
            .putTableMigrationStateMap(TABLE, TableMigrationState.builder()
                    .migrationsState(STATE)
                    .build())
            .putTableMigrationStateMap(OTHER_TABLE, TableMigrationState.builder()
                    .migrationsState(OTHER_STATE)
                    .build())
            .build();
    private static final long TIMESTAMP = 10L;
    private static final boolean TRANSFORMATION_RESULT = true;

    private final CoordinationServiceImpl<TableMigrationStateMap> coordinationService =
            mock(CoordinationServiceImpl.class);
    private final MigrationCoordinationStateTransformer stateTransformer =
            mock(MigrationCoordinationStateTransformer.class);

    private final MigrationCoordinationService migrationCoordinationService =
            new MigrationCoordinationServiceImpl(coordinationService, stateTransformer);

    @Test
    public void testStartMigrationCallsStateTransformerWithCorrectArgs() {
        when(stateTransformer.transformMigrationStateForTable(
                TABLE,
                Optional.of(TARGET_TABLE),
                MigrationState.WRITE_BOTH_READ_FIRST)
        ).thenReturn(TRANSFORMATION_RESULT);

        assertThat(migrationCoordinationService.startMigration(TABLE, TARGET_TABLE))
                .isEqualTo(TRANSFORMATION_RESULT);
    }

    @Test
    public void testEndMigrationCallsStateTransformerWithCorrectArgs() {
        when(stateTransformer.transformMigrationStateForTable(
                TABLE,
                Optional.empty(),
                MigrationState.WRITE_BOTH_READ_SECOND)
        ).thenReturn(TRANSFORMATION_RESULT);

        assertThat(migrationCoordinationService.endMigration(TABLE))
                .isEqualTo(TRANSFORMATION_RESULT);
    }

    @Test
    public void testEndDualWriteCallsStateTransformerWithCorrectArgs() {
        when(stateTransformer.transformMigrationStateForTable(
                TABLE,
                Optional.empty(),
                MigrationState.WRITE_SECOND_READ_SECOND)
        ).thenReturn(TRANSFORMATION_RESULT);

        assertThat(migrationCoordinationService.endDualWrite(TABLE))
                .isEqualTo(TRANSFORMATION_RESULT);
    }

    @Test
    public void testGetMigrationStateReturnsDefaultStateWhenStateTableNotInitialized() {
        ValueAndBound<TableMigrationStateMap> currentValue = ValueAndBound.of(Optional.empty(), TIMESTAMP);

        when(coordinationService.getValueForTimestamp(TIMESTAMP)).thenReturn(Optional.of(currentValue));

        assertThat(migrationCoordinationService.getMigrationState(TABLE, TIMESTAMP))
                .isEqualTo(DEFAULT_TABLE_MIGRATION_STATE);
    }

    @Test
    public void testGetMigrationStateReturnsDefaultStateWhenStateTableDoestHaveTheRequestedTableState() {
        TableMigrationStateMap stateMapWithoutRequestedTable = TableMigrationStateMap.builder()
                .putTableMigrationStateMap(OTHER_TABLE, TableMigrationState.builder()
                        .migrationsState(OTHER_STATE)
                        .build())
                .build();
        ValueAndBound<TableMigrationStateMap> currentValue =
                ValueAndBound.of(Optional.of(stateMapWithoutRequestedTable), TIMESTAMP);

        when(coordinationService.getValueForTimestamp(TIMESTAMP)).thenReturn(Optional.of(currentValue));

        assertThat(migrationCoordinationService.getMigrationState(TABLE, TIMESTAMP))
                .isEqualTo(DEFAULT_TABLE_MIGRATION_STATE);
    }

    @Test
    public void testGetMigrationStateReturnsExpectedValueFromCoordinationService() {
        ValueAndBound<TableMigrationStateMap> currentValue = ValueAndBound.of(STATE_MAP, TIMESTAMP);

        when(coordinationService.getValueForTimestamp(TIMESTAMP)).thenReturn(Optional.of(currentValue));

        TableMigrationState expected = TableMigrationState.of(STATE);

        assertThat(migrationCoordinationService.getMigrationState(TABLE, TIMESTAMP))
                .isEqualTo(expected);
    }
}

