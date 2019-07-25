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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.palantir.atlasdb.migration.MigrationStateTransformer.EMPTY_TABLE_MIGRATION_STATE_MAP;

import java.util.Optional;
import java.util.function.Function;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.coordination.CoordinationServiceImpl;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

@SuppressWarnings("unchecked") // Mocks of generic types
public class MigrationStateTransformerTest {
    private static final TableReference TABLE = TableReference.fromString("table.some");
    private static final MigrationState STATE = MigrationState.WRITE_FIRST_ONLY;
    private static final MigrationState TARGET_STATE = MigrationState.WRITE_BOTH_READ_FIRST;

    private static final Optional<TableReference> MAYBE_TARGET_TABLE =
            Optional.of(TableReference.fromString("table.target"));
    private static final TableReference OTHER_TABLE = TableReference.fromString("table.other");

    private static final TableMigrationStateMap STATE_MAP = TableMigrationStateMap.builder()
            .putTableMigrationStateMap(TABLE, TableMigrationState.builder()
                    .migrationsState(STATE)
                    .targetTable(TableReference.fromString("table.targetTable"))
                    .build())
            .putTableMigrationStateMap(OTHER_TABLE, TableMigrationState.builder()
                    .migrationsState(STATE)
                    .targetTable(TableReference.fromString("table.targetother"))
                    .build())
            .build();
    private static final TableMigrationStateMap NEW_STATE_MAP = TableMigrationStateMap.builder()
            .putTableMigrationStateMap(TABLE, TableMigrationState.builder()
                    .migrationsState(TARGET_STATE)
                    .targetTable(TableReference.fromString("table.targetTable"))
                    .build())
            .putTableMigrationStateMap(OTHER_TABLE, TableMigrationState.builder()
                    .migrationsState(STATE)
                    .targetTable(TableReference.fromString("table.targetother"))
                    .build())
            .build();
    private static final long TIMESTAMP = 10L;
    private static final boolean SUCCESSFUL = true;
    private static final boolean FAILED = false;

    private final CoordinationServiceImpl<TableMigrationStateMap> coordinationService =
            mock(CoordinationServiceImpl.class);
    private final MigrationStateTransitioner stateTransitioner = mock(MigrationStateTransitioner.class);
    private final MigrationStateTransformer stateTransformer =
            new MigrationStateTransformer(stateTransitioner, coordinationService);

    @Test
    public void testTransformCallsCoordinationServiceWithCorrectArguments() {
        ValueAndBound<TableMigrationStateMap> currentValue = ValueAndBound.of(STATE_MAP, TIMESTAMP);
        ValueAndBound<TableMigrationStateMap> transformedValue = ValueAndBound.of(NEW_STATE_MAP, TIMESTAMP);

        CheckAndSetResult<ValueAndBound<TableMigrationStateMap>> result =
                CheckAndSetResult.of(true, ImmutableList.of(transformedValue));

        when(coordinationService.tryTransformCurrentValue(any()))
                .thenReturn(result);

        ArgumentCaptor<Function> functionArgumentCaptor = ArgumentCaptor.forClass(Function.class);

        stateTransformer.transformMigrationStateForTable(TABLE, MAYBE_TARGET_TABLE, TARGET_STATE);
        verify(coordinationService).tryTransformCurrentValue(functionArgumentCaptor.capture());

        functionArgumentCaptor.getValue().apply(currentValue);
        verify(stateTransitioner).updateTableMigrationStateForTable(STATE_MAP, TABLE, MAYBE_TARGET_TABLE, TARGET_STATE);
    }

    @Test
    public void testTransformCreatesInitialStateTable() {
        ValueAndBound<TableMigrationStateMap> currentValue = ValueAndBound.of(Optional.empty(), TIMESTAMP);
        ValueAndBound<TableMigrationStateMap> transformedValue = ValueAndBound.of(NEW_STATE_MAP, TIMESTAMP);

        CheckAndSetResult<ValueAndBound<TableMigrationStateMap>> result =
                CheckAndSetResult.of(true, ImmutableList.of(transformedValue));

        when(coordinationService.tryTransformCurrentValue(any()))
                .thenReturn(result);

        ArgumentCaptor<Function> functionArgumentCaptor = ArgumentCaptor.forClass(Function.class);

        stateTransformer.transformMigrationStateForTable(TABLE, MAYBE_TARGET_TABLE, TARGET_STATE);
        verify(coordinationService).tryTransformCurrentValue(functionArgumentCaptor.capture());

        functionArgumentCaptor.getValue().apply(currentValue);
        verify(stateTransitioner)
                .updateTableMigrationStateForTable(
                        EMPTY_TABLE_MIGRATION_STATE_MAP,
                        TABLE,
                        MAYBE_TARGET_TABLE,
                        TARGET_STATE);
    }

    @Test
    public void testTransformReturnsTrueWhenTransformationSucceededAndFinalResultIsWhatWeWanted() {
        ValueAndBound<TableMigrationStateMap> transformedValue = ValueAndBound.of(NEW_STATE_MAP, TIMESTAMP);

        CheckAndSetResult<ValueAndBound<TableMigrationStateMap>> result =
                CheckAndSetResult.of(SUCCESSFUL, ImmutableList.of(transformedValue));

        when(coordinationService.tryTransformCurrentValue(any()))
                .thenReturn(result);

        assertThat(stateTransformer.transformMigrationStateForTable(TABLE, MAYBE_TARGET_TABLE, TARGET_STATE))
                .isEqualTo(SUCCESSFUL);
    }

    @Test
    public void testTransformReturnsFalseWhenTransformationSucceededButFinalStateIsNotWhatWeWanted() {
        ValueAndBound<TableMigrationStateMap> transformedValue = ValueAndBound.of(STATE_MAP, TIMESTAMP);

        CheckAndSetResult<ValueAndBound<TableMigrationStateMap>> result =
                CheckAndSetResult.of(SUCCESSFUL, ImmutableList.of(transformedValue));

        when(coordinationService.tryTransformCurrentValue(any()))
                .thenReturn(result);

        assertThat(stateTransformer.transformMigrationStateForTable(TABLE, MAYBE_TARGET_TABLE, TARGET_STATE))
                .isEqualTo(FAILED);
    }

    @Test
    public void testTransformReturnsTrueWhenTransformationFailedButFinalStateIsWhatWeWanted() {
        ValueAndBound<TableMigrationStateMap> transformedValue = ValueAndBound.of(NEW_STATE_MAP, TIMESTAMP);

        CheckAndSetResult<ValueAndBound<TableMigrationStateMap>> result =
                CheckAndSetResult.of(FAILED, ImmutableList.of(transformedValue));

        when(coordinationService.tryTransformCurrentValue(any()))
                .thenReturn(result);

        assertThat(stateTransformer.transformMigrationStateForTable(TABLE, MAYBE_TARGET_TABLE, TARGET_STATE))
                .isEqualTo(SUCCESSFUL);
    }

    @Test
    public void testTransformThrowsWhenTheResultHasNoTableStateMap() {
        ValueAndBound<TableMigrationStateMap> transformedValue = ValueAndBound.of(Optional.empty(), TIMESTAMP);

        CheckAndSetResult<ValueAndBound<TableMigrationStateMap>> result =
                CheckAndSetResult.of(true, ImmutableList.of(transformedValue));

        when(coordinationService.tryTransformCurrentValue(any()))
                .thenReturn(result);

        assertThatExceptionOfType(SafeIllegalStateException.class)
                .isThrownBy(() ->
                        stateTransformer.transformMigrationStateForTable(TABLE, MAYBE_TARGET_TABLE, TARGET_STATE));

    }

    @Test
    public void testTransformThrowsWhenTheResultHasNoTableStateForUpdatedTable() {
        TableMigrationStateMap stateMapWithoutUpdatedTable = TableMigrationStateMap.builder()
                .putTableMigrationStateMap(OTHER_TABLE, TableMigrationState.builder()
                        .migrationsState(STATE)
                        .build())
                .build();

        ValueAndBound<TableMigrationStateMap> transformedValue =
                ValueAndBound.of(stateMapWithoutUpdatedTable, TIMESTAMP);

        CheckAndSetResult<ValueAndBound<TableMigrationStateMap>> result =
                CheckAndSetResult.of(true, ImmutableList.of(transformedValue));

        when(coordinationService.tryTransformCurrentValue(any()))
                .thenReturn(result);

        assertThatExceptionOfType(SafeIllegalStateException.class)
                .isThrownBy(() ->
                        stateTransformer.transformMigrationStateForTable(TABLE, MAYBE_TARGET_TABLE, TARGET_STATE));

    }

}