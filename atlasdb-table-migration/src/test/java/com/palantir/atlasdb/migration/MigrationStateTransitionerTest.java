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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

public class MigrationStateTransitionerTest {
    private static final List<MigrationState> ALL_STATES = Arrays.asList(MigrationState.values());
    private static final Map<MigrationState, MigrationState> VALID_TRANSITIONS = getValidMap();

    private final MigrationStateTransitioner stateTransitioner = new MigrationStateTransitioner();
    private static final TableReference START_TABLE = TableReference.fromString("table.start");
    private static final TableReference TARGET_TABLE = TableReference.fromString("table.target");
    private static final TableReference OTHER_TABLE = TableReference.fromString("table.other");
    private static final MigrationState OTHER_STATE = MigrationState.WRITE_BOTH_READ_SECOND;

    @Test
    public void returnsExpectedValuesForAllValidTransitionsThrowsOtherwise() {
        for (int i = 0; i < ALL_STATES.size(); i++) {
            for (int j = 0; j < ALL_STATES.size(); j++) {
                MigrationState startState = ALL_STATES.get(i);
                MigrationState targetState = ALL_STATES.get(j);

                TableMigrationStateMap initialStateMap = getInitialStateMap(START_TABLE, startState, Optional.empty());

                if (Objects.equals(VALID_TRANSITIONS.get(startState), targetState)) {
                    Optional<TableReference> maybeTargetTable = getMaybeTargetTable(startState, targetState);

                    TableMigrationStateMap expected = getExpectedTableMigrationStateMap(
                            initialStateMap,
                            START_TABLE,
                            TableMigrationState.builder()
                                    .migrationsState(targetState)
                                    .targetTable(maybeTargetTable)
                                    .build());

                    assertThat(getUpdatedStateMap(targetState, maybeTargetTable, initialStateMap))
                            .isEqualTo(expected);

                    assertThatExceptionOfType(SafeIllegalStateException.class).isThrownBy(() ->
                            getUpdatedStateMap(targetState, flipTargetTable(maybeTargetTable), initialStateMap));

                } else {
                    assertThatExceptionOfType(SafeIllegalStateException.class)
                            .isThrownBy(() -> getUpdatedStateMap(targetState, Optional.empty(), initialStateMap));
                }
            }
        }
    }

    private TableMigrationStateMap getUpdatedStateMap(MigrationState targetState,
            Optional<TableReference> maybeTargetTable,
            TableMigrationStateMap initialState) {
        return stateTransitioner.updateTableMigrationStateForTable(
                initialState,
                START_TABLE,
                maybeTargetTable,
                targetState);
    }

    private static Optional<TableReference> flipTargetTable(Optional<TableReference> maybeTargetTable) {
        if (maybeTargetTable.isPresent()) {
            return Optional.empty();
        } else {
            return Optional.of(TARGET_TABLE);
        }
    }

    private static Map<MigrationState, MigrationState> getValidMap() {
        Map<MigrationState, MigrationState> validTransitions = new HashMap<>();

        validTransitions.put(MigrationState.WRITE_FIRST_ONLY, MigrationState.WRITE_BOTH_READ_FIRST);
        validTransitions.put(MigrationState.WRITE_BOTH_READ_FIRST, MigrationState.WRITE_BOTH_READ_SECOND);
        validTransitions.put(MigrationState.WRITE_BOTH_READ_SECOND, MigrationState.WRITE_SECOND_READ_SECOND);

        return validTransitions;
    }

    private static TableMigrationStateMap getInitialStateMap(
            TableReference tableReference,
            MigrationState targetState,
            Optional<TableReference> maybeTargetTable) {
        return TableMigrationStateMap.builder()
                .putTableMigrationStateMap(OTHER_TABLE, TableMigrationState.builder()
                        .migrationsState(OTHER_STATE)
                        .targetTable(TableReference.fromString("table.targetother"))
                        .build())
                .putTableMigrationStateMap(tableReference, TableMigrationState.builder()
                        .targetTable(maybeTargetTable)
                        .migrationsState(targetState)
                        .build())
                .build();
    }

    private static TableMigrationStateMap getExpectedTableMigrationStateMap(
            TableMigrationStateMap current,
            TableReference tableReference,
            TableMigrationState tableMigrationState) {
        Map<TableReference, TableMigrationState> newStateMap = new HashMap<>(current.tableMigrationStateMap());
        newStateMap.put(tableReference, tableMigrationState);
        return TableMigrationStateMap.builder().tableMigrationStateMap(newStateMap).build();
    }

    private static Optional<TableReference> getMaybeTargetTable(MigrationState startState, MigrationState targetState) {
        Optional<TableReference> maybeTargetTable = Optional.empty();
        if (startState.equals(MigrationState.WRITE_FIRST_ONLY)
                && targetState.equals(MigrationState.WRITE_BOTH_READ_FIRST)) {
            maybeTargetTable = Optional.of(TARGET_TABLE);
        }
        return maybeTargetTable;
    }
}
