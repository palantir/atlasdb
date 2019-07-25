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

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

public class MigrationStateTransformer {
    private static final Logger log = LoggerFactory.getLogger(MigrationStateTransformer.class);

    private static final TableMigrationStateMap EMPTY_TABLE_MIGRATION_STATE_MAP =
            ImmutableTableMigrationStateMap.builder().build();
    private final MigrationStateTransitioner migrationStateTransitioner;
    private final CoordinationService<TableMigrationStateMap> coordinationService;

    public MigrationStateTransformer(
            MigrationStateTransitioner migrationStateTransitioner,
            CoordinationService<TableMigrationStateMap> coordinationService) {
        this.migrationStateTransitioner = migrationStateTransitioner;
        this.coordinationService = coordinationService;
    }

    public boolean transformMigrationStateForTable(
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

        TableMigrationState finalState = Optional.ofNullable(Iterables.getOnlyElement(transformResult.existingValues())
                .value()
                .orElseThrow(() -> new SafeIllegalStateException("Unexpectedly found no value in store"))
                .tableMigrationStateMap()
                .get(startTable))
                .orElseThrow(() -> new SafeIllegalStateException("Unexpectedly found no value in store"));

        if (transformResult.successful() && finalState.migrationsState() == targetState) {
            log.info(
                    "We attempted to change migration state for table {} and this was successful.",
                    LoggingArgs.tableRef(startTable));
            return true;
        }

        if (finalState.migrationsState() == targetState) {
            log.info("We attempted to change migration state for table {}."
                            + "We failed, but the table is already in this state anyway.",
                    LoggingArgs.tableRef(startTable));
            return true;
        }

        return false;

    }

    TableMigrationStateMap getCurrentTableMigrationStateMap(ValueAndBound<TableMigrationStateMap> valueAndBound) {
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