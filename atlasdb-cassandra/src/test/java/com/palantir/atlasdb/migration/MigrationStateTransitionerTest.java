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
import java.util.Optional;

import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

public class MigrationStateTransitionerTest {
    private static final List<MigrationCoordinationService.MigrationState> ALL_STATES =
            Arrays.asList(MigrationCoordinationService.MigrationState.values());
    private static final Map<MigrationCoordinationService.MigrationState, MigrationCoordinationService.MigrationState>
            VALID_TRANSITIONS = getValidMap();

    private final MigrationStateTransitioner stateTransitioner = new MigrationStateTransitioner();

    @Test
    public void returnsExectedValuesForAllValidTransitionsThrowsOtherwise() {
        TableReference startTable = TableReference.fromString("table.start");
        TableReference targetTable = TableReference.fromString("table.target");
        TableReference otherTable = TableReference.fromString("table.start");
        MigrationCoordinationService.MigrationState otherState =
                MigrationCoordinationService.MigrationState.WRITE_BOTH_READ_SECOND;
        TableMigrationStateMap initialStateMap = TableMigrationStateMap.builder()
                .putTableMigrationStateMap(otherTable, TableMigrationState.of(otherState))
                .build();
        for (int i = 0; i < ALL_STATES.size(); i++) {
            for (int j = 0; j < ALL_STATES.size(); j++) {
                MigrationCoordinationService.MigrationState startState = ALL_STATES.get(i);
                MigrationCoordinationService.MigrationState targetState = ALL_STATES.get(j);

                if (VALID_TRANSITIONS.get(startState).equals(targetState)) {
                    assertThat(
                            stateTransitioner.updateTableMigrationStateForTable(
                                    initialStateMap,
                                    startTable,
                                    Optional.empty(),
                                    targetState)
                    ).isEqualTo(getExpectedUpdatedMap(
                            initialStateMap,
                            startTable,
                            TableMigrationState.of(targetState))
                    );
                } else {
                    assertThatExceptionOfType(SafeIllegalStateException.class)
                            .isThrownBy(() -> stateTransitioner.updateTableMigrationStateForTable(
                                    initialStateMap,
                                    startTable,
                                    Optional.empty(),
                                    targetState));
                }
            }
        }
    }

    private TableMigrationStateMap getExpectedUpdatedMap(
            TableMigrationStateMap current,
            TableReference tableReference,
            TableMigrationState tableMigrationState) {
        Map<TableReference, TableMigrationState> newStateMap = new HashMap<>(current.tableMigrationStateMap());
        newStateMap.put(tableReference, tableMigrationState);
        return TableMigrationStateMap.builder().tableMigrationStateMap(newStateMap).build();
    }

    private static Map<MigrationCoordinationService.MigrationState, MigrationCoordinationService.MigrationState>
    getValidMap() {
        Map<MigrationCoordinationService.MigrationState, MigrationCoordinationService.MigrationState> validTransitions =
                new HashMap<>();

        validTransitions.put(MigrationCoordinationService.MigrationState.WRITE_FIRST_ONLY,
                MigrationCoordinationService.MigrationState.WRITE_BOTH_READ_FIRST);
        validTransitions
                .put(MigrationCoordinationService.MigrationState.WRITE_BOTH_READ_FIRST,
                        MigrationCoordinationService.MigrationState.WRITE_BOTH_READ_SECOND);
        validTransitions.put(MigrationCoordinationService.MigrationState.WRITE_BOTH_READ_SECOND,
                MigrationCoordinationService.MigrationState.WRITE_SECOND_READ_SECOND);

        return validTransitions;
    }
}
