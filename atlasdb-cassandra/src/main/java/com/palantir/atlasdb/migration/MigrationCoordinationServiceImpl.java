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

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.coordination.CoordinationServiceImpl;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult;

public class MigrationCoordinationServiceImpl implements MigrationCoordinationService {
    static final MigrationState DEFAULT_MIGRATIONS_STATE = MigrationState.WRITE_FIRST_ONLY;

    private static final Logger log = LoggerFactory.getLogger(MigrationCoordinationServiceImpl.class);

    private static final TableMigrationStateMap EMPTY_TABLE_MIGRATION_STATE_MAP =
            ImmutableTableMigrationStateMap.builder().build();
    private static final TableMigrationState DEFAULT_TABLE_MIGRATION_STATE =
            TableMigrationState.of(DEFAULT_MIGRATIONS_STATE);

    private final CoordinationServiceImpl<TableMigrationStateMap> coordinationService;
    private final MigrationStateTransitioner migrationStateTransitioner;

    public MigrationCoordinationServiceImpl(
            CoordinationServiceImpl<TableMigrationStateMap> coordinationService,
            MigrationStateTransitioner migrationStateTransitioner) {
        this.coordinationService = coordinationService;
        this.migrationStateTransitioner = migrationStateTransitioner;
    }

    @Override
    public boolean startMigration(TableReference startTable, TableReference targetTable) {
        return safeTryChangeState(startTable, Optional.of(targetTable), MigrationState.WRITE_BOTH_READ_FIRST);
    }

    @Override
    public boolean endMigration(TableReference startTable) {
        return safeTryChangeState(startTable, Optional.empty(), MigrationState.WRITE_BOTH_READ_SECOND);
    }

    @Override
    public boolean endDualWrite(TableReference startTable) {
        return safeTryChangeState(startTable, Optional.empty(), MigrationState.WRITE_SECOND_READ_SECOND);
    }

    @Override
    public TableMigrationState getMigrationState(TableReference startTable, long timestamp) {
        Optional<ValueAndBound<TableMigrationStateMap>> maybeTableMigrationStateMapValueAndBound =
                coordinationService.getValueForTimestamp(timestamp);

        if (!maybeTableMigrationStateMapValueAndBound.isPresent()) {
            return DEFAULT_TABLE_MIGRATION_STATE;
        }

        ValueAndBound<TableMigrationStateMap> tableMigrationStateMapValueAndBound =
                maybeTableMigrationStateMapValueAndBound.get();

        return tableMigrationStateMapValueAndBound.value()
                .map(value ->
                        value.tableMigrationStateMap().getOrDefault(startTable, DEFAULT_TABLE_MIGRATION_STATE))
                .orElse(DEFAULT_TABLE_MIGRATION_STATE);
    }

    private boolean safeTryChangeState(
            TableReference startTable,
            Optional<TableReference> targetTable,
            MigrationState targetState) {
        CheckAndSetResult<ValueAndBound<TableMigrationStateMap>> transformResult =
                coordinationService.tryTransformCurrentValue(valueAndBound ->
                        migrationStateTransitioner.updateTableMigrationStateForTable(
                                getCurrentTableMigrationStateMap(valueAndBound),
                                startTable,
                                targetTable,
                                targetState));

        //todo(jelenac): see TransactionSchemaManager.tryInstallNewTransactionsSchemaVersion for defensive logic
        return true;
    }

    private TableMigrationStateMap getCurrentTableMigrationStateMap(
            ValueAndBound<TableMigrationStateMap> valueAndBound) {
        if (!valueAndBound.value().isPresent()) {
            log.warn(
                    "Attempting to change migration state for the table, but no past data was found,"
                            + ".This should normally only happen once per"
                            + " server, and only on or around first startup since upgrading to a version of AtlasDB"
                            + " that is aware of the sweep by migration."
                            + " If this message persists, please contact support.");
            return EMPTY_TABLE_MIGRATION_STATE_MAP;
        } else {
            return valueAndBound.value().get();
        }
    }
}
