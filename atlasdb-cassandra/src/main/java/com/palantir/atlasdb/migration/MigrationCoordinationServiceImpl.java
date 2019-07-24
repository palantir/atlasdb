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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.coordination.CoordinationServiceImpl;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult;
import com.palantir.atlasdb.keyvalue.impl.TableMigratingKeyValueService;
import com.palantir.atlasdb.logging.LoggingArgs;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class MigrationCoordinationServiceImpl implements MigrationCoordinationService {
    private static final Logger log = LoggerFactory.getLogger(MigrationCoordinationServiceImpl.class);

    private static final TableMigratingKeyValueService.MigrationsState DEFAULT_MIGRATIONS_STATE =
            TableMigratingKeyValueService.MigrationsState.WRITE_FIRST_ONLY;
    private static final TableMigrationState DEFAULT_TABLE_MIGRATION_STATE =
            TableMigrationState.of(DEFAULT_MIGRATIONS_STATE);
    private final CoordinationServiceImpl<TableMigrationStateMap> coordinationService;

    public MigrationCoordinationServiceImpl(CoordinationServiceImpl<TableMigrationStateMap> coordinationService) {
        this.coordinationService = coordinationService;
    }

    @Override
    public boolean startMigration(TableReference startTable, TableReference targetTable) {
        CheckAndSetResult<ValueAndBound<TableMigrationStateMap>> transformResult =
                tryStartMigration(startTable, targetTable);


        //todo(jelenac): see TransactionSchemaManager.tryInstallNewTransactionsSchemaVersion for defensive logic
        return true;

    }

    private CheckAndSetResult<ValueAndBound<TableMigrationStateMap>> tryStartMigration(
            TableReference startTable,
            TableReference targetTable) {
        return coordinationService.tryTransformCurrentValue(
                valueAndBound -> changeStateForTableOrDefault(
                        startTable,
                        Optional.of(targetTable),
                        TableMigratingKeyValueService.MigrationsState.WRITE_BOTH_READ_FIRST,
                        valueAndBound));
    }

    private TableMigrationStateMap changeStateForTableOrDefault(
            TableReference startTable,
            Optional<TableReference> targetTable,
            TableMigratingKeyValueService.MigrationsState migrationsState,
            ValueAndBound<TableMigrationStateMap> valueAndBound) {

        if (!valueAndBound.value().isPresent()) {
            log.warn("Attempting to change migration state for the table {}, but no past data was found,"
                            + ".This should normally only happen once per"
                            + " server, and only on or around first startup since upgrading to a version of AtlasDB"
                            + " that is aware of the sweep by migration."
                            + " If this message persists, please contact support.",
                    LoggingArgs.tableRef(startTable));
            return TableMigrationStateMap.builder()
                    .putTableMigrationStateMap(startTable, TableMigrationState.builder()
                            .migrationsState(migrationsState)
                            .targetTable(targetTable)
                            .build())
                    .build();
        }

        log.info("Changing migration state for the table {}", LoggingArgs.tableRef(startTable));

        TableMigrationStateMap tableMigrationStateMap = valueAndBound.value().get();
        return getUpdatedStateMap(startTable, targetTable, migrationsState, tableMigrationStateMap);

    }

    private TableMigrationStateMap getUpdatedStateMap(
            TableReference startTable,
            Optional<TableReference> targetTable,
            TableMigratingKeyValueService.MigrationsState migrationsState,
            TableMigrationStateMap tableMigrationStateMap) {
        Map<TableReference, TableMigrationState> newStateMap =
                new HashMap<>(tableMigrationStateMap.tableMigrationStateMap());
        newStateMap.put(startTable, TableMigrationState.builder()
                .migrationsState(migrationsState)
                .targetTable(targetTable)
                .build());

        return TableMigrationStateMap.builder()
                .tableMigrationStateMap(newStateMap)
                .build();
    }

    @Override
    public boolean endMigration(TableReference startTable) {
        throw new NotImplementedException();

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
}
