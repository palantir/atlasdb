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

import java.util.Set;
import java.util.function.LongSupplier;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class MigratingTableMapperServiceImpl implements MigratingTableMapperService {
    private final Set<TableReference> tablesToMigrate;
    private final LongSupplier immutableTsSupplier;
    private MigrationCoordinationService coordinationService;

    public MigratingTableMapperServiceImpl(Set<TableReference> tablesToMigrate, LongSupplier immutableTsSupplier) {
        this.tablesToMigrate = tablesToMigrate;
        this.immutableTsSupplier = immutableTsSupplier;
    }

    @Override
    public TableReference readTable(TableReference startTable) {
        if (!tablesToMigrate.contains(startTable)) {
            return startTable;
        }
        TableMigrationState migrationState = coordinationService.getMigrationState(startTable,
                immutableTsSupplier.getAsLong());
        switch (migrationState.migrationsState()) {
            case WRITE_FIRST_ONLY:
            case WRITE_BOTH_READ_FIRST:
                return startTable;
            case WRITE_BOTH_READ_SECOND:
            case WRITE_SECOND_READ_SECOND:
                return migrationState.targetTable().get();
            default:
                throw new IllegalStateException("Unknown migration state");
        }
    }

    @Override
    public Set<TableReference> writeTables(TableReference startTable) {
        if (!tablesToMigrate.contains(startTable)) {
            return ImmutableSet.of(startTable);
        }
        TableMigrationState migrationState = coordinationService.getMigrationState(startTable,
                immutableTsSupplier.getAsLong());
        switch (migrationState.migrationsState()) {
            case WRITE_FIRST_ONLY:
                return ImmutableSet.of(startTable);
            case WRITE_BOTH_READ_FIRST:
            case WRITE_BOTH_READ_SECOND:
                return ImmutableSet.of(startTable, migrationState.targetTable().get());
            case WRITE_SECOND_READ_SECOND:
                return ImmutableSet.of(migrationState.targetTable().get());
            default:
                throw new IllegalStateException("Unknown migration state");
        }
    }

    public void initialize(CoordinationService<TableMigrationStateMap> coordinator) {
        coordinationService = new MigrationCoordinationServiceImpl(coordinator, new MigrationStateTransitioner());
    }
}
