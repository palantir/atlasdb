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

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.TableMigratingKeyValueService;

@Value.Immutable
@JsonDeserialize(as = ImmutableTableMigrationState.class)
@JsonSerialize(as = ImmutableTableMigrationState.class)
public abstract class TableMigrationState {
    @Value.Parameter
    public abstract TableMigratingKeyValueService.MigrationsState migrationsState();

    @Value.Parameter
    public abstract Optional<TableReference> targetTable();

    public static ImmutableTableMigrationState of(TableMigratingKeyValueService.MigrationsState state) {
        return builder()
                .migrationsState(state)
                .build();
    }

    public static ImmutableTableMigrationState of(
            TableMigratingKeyValueService.MigrationsState state,
            TableReference targetTable) {
        return builder()
                .migrationsState(state)
                .targetTable(targetTable)
                .build();
    }

    public static ImmutableTableMigrationState.Builder builder() {
        return ImmutableTableMigrationState.builder();
    }
}
