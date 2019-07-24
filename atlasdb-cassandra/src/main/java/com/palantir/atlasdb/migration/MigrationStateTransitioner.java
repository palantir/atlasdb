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

import static com.palantir.atlasdb.migration.MigrationCoordinationServiceImpl.DEFAULT_MIGRATIONS_STATE;
import static com.palantir.atlasdb.migration.MigrationCoordinationServiceImpl.MigrationState.WRITE_BOTH_READ_FIRST;
import static com.palantir.atlasdb.migration.MigrationCoordinationServiceImpl.MigrationState.WRITE_BOTH_READ_SECOND;
import static com.palantir.atlasdb.migration.MigrationCoordinationServiceImpl.MigrationState.WRITE_FIRST_ONLY;
import static com.palantir.atlasdb.migration.MigrationCoordinationServiceImpl.MigrationState.WRITE_SECOND_READ_SECOND;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

public class MigrationStateTransitioner {
    private static final Logger log = LoggerFactory.getLogger(MigrationCoordinationServiceImpl.class);

    public TableMigrationStateMap updateTableMigrationStateForTable(
            TableMigrationStateMap tableMigrationStateMap,
            TableReference startTable,
            Optional<TableReference> targetTable,
            MigrationCoordinationServiceImpl.MigrationState targetState) {

        log.info("Changing migration state for the table {}",
                LoggingArgs.tableRef(startTable));

        Map<TableReference, TableMigrationState> currentStateMap = tableMigrationStateMap.tableMigrationStateMap();
        Map<TableReference, TableMigrationState> newStateMap = new HashMap<>(currentStateMap);

        MigrationCoordinationServiceImpl.MigrationState currentState = Optional.ofNullable(
                currentStateMap.get(startTable))
                .map(TableMigrationState::migrationsState)
                .orElse(DEFAULT_MIGRATIONS_STATE);

        if (!isTransitionValid(currentState, targetState, targetTable.isPresent())) {
            throw new SafeIllegalStateException("Invalid state migration transition requested");
        }

        newStateMap.put(startTable, TableMigrationState.builder()
                .migrationsState(targetState)
                .targetTable(targetTable)
                .build());

        return TableMigrationStateMap.builder()
                .tableMigrationStateMap(newStateMap)
                .build();
    }

    private boolean isTransitionValid(
            MigrationCoordinationServiceImpl.MigrationState currentState,
            MigrationCoordinationServiceImpl.MigrationState targetState,
            boolean targetTableGiven) {
        if (currentState.equals(WRITE_FIRST_ONLY)) {
            return targetState.equals(WRITE_BOTH_READ_FIRST) && targetTableGiven;
        }

        if (targetTableGiven) {
            // todo(jelenac): Should we throw illegal state exception here? could return false and let the check above throw
            log.warn("Target table given for a migration when it's not needed. The given table will be ignored");
        }

        if (currentState.equals(WRITE_BOTH_READ_FIRST)) {
            return targetState.equals(WRITE_BOTH_READ_SECOND);
        }

        if (currentState.equals(WRITE_BOTH_READ_SECOND)) {
            return targetState.equals(WRITE_SECOND_READ_SECOND);
        }

        return false;
    }
}