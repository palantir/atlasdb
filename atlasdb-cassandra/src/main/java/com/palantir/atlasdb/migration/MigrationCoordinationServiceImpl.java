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

import static com.palantir.atlasdb.keyvalue.impl.TableMigratingKeyValueService.MigrationsState.WRITE_BOTH_READ_FIRST;
import static com.palantir.atlasdb.keyvalue.impl.TableMigratingKeyValueService.MigrationsState.WRITE_BOTH_READ_SECOND;
import static com.palantir.atlasdb.keyvalue.impl.TableMigratingKeyValueService.MigrationsState.WRITE_FIRST_ONLY;
import static com.palantir.atlasdb.keyvalue.impl.TableMigratingKeyValueService.MigrationsState.WRITE_SECOND_READ_SECOND;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.coordination.CoordinationServiceImpl;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult;
import com.palantir.atlasdb.keyvalue.impl.TableMigratingKeyValueService;

public class MigrationCoordinationServiceImpl implements MigrationCoordinationService {
    public static final TableMigratingKeyValueService.MigrationsState DEFAULT_MIGRATIONS_STATE = WRITE_FIRST_ONLY;

    private static final Logger log = LoggerFactory.getLogger(MigrationCoordinationServiceImpl.class);

    private static final TableMigrationState DEFAULT_TABLE_MIGRATION_STATE =
            TableMigrationState.of(DEFAULT_MIGRATIONS_STATE);
    private final CoordinationServiceImpl<TableMigrationStateMap> coordinationService;
    private final MigrationStateTransitioner migrationStateTransitioner = new MigrationStateTransitioner();

    public MigrationCoordinationServiceImpl(CoordinationServiceImpl<TableMigrationStateMap> coordinationService) {
        this.coordinationService = coordinationService;
    }

    @Override
    public boolean startMigration(TableReference startTable, TableReference targetTable) {
        CheckAndSetResult<ValueAndBound<TableMigrationStateMap>> transformResult =
                coordinationService.tryTransformCurrentValue(valueAndBound ->
                        migrationStateTransitioner.changeStateForTable(
                                startTable,
                                Optional.of(targetTable),
                                WRITE_BOTH_READ_FIRST,
                                valueAndBound));

        //todo(jelenac): see TransactionSchemaManager.tryInstallNewTransactionsSchemaVersion for defensive logic
        return true;

    }

    @Override
    public boolean endMigration(TableReference startTable) {
        CheckAndSetResult<ValueAndBound<TableMigrationStateMap>> transformResult =
                coordinationService.tryTransformCurrentValue(valueAndBound ->
                        migrationStateTransitioner.changeStateForTable(
                                startTable,
                                Optional.empty(),
                                WRITE_BOTH_READ_SECOND,
                                valueAndBound));
        //todo(jelenac): see TransactionSchemaManager.tryInstallNewTransactionsSchemaVersion for defensive logic
        return true;
    }

    @Override
    public boolean endDualWrite(TableReference startTable) {
        CheckAndSetResult<ValueAndBound<TableMigrationStateMap>> transformResult =
                coordinationService.tryTransformCurrentValue(valueAndBound ->
                        migrationStateTransitioner.changeStateForTable(
                                startTable,
                                Optional.empty(),
                                WRITE_SECOND_READ_SECOND,
                                valueAndBound));
        //todo(jelenac): see TransactionSchemaManager.tryInstallNewTransactionsSchemaVersion for defensive logic
        return true;
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
