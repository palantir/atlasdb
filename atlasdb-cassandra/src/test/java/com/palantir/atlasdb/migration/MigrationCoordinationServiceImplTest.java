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

import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.TableMigratingKeyValueService;

public class MigrationCoordinationServiceImplTest {
    @Test
    public void test() {
        TableReference tableReference1 = TableReference.fromString("table.table1");
        TableReference tableReference2 = TableReference.fromString("table.table2");

        TableMigrationState state1 = TableMigrationState.of(TableMigratingKeyValueService.MigrationsState.WRITE_BOTH_READ_FIRST);
        TableMigrationState state2 = TableMigrationState.of(TableMigratingKeyValueService.MigrationsState.WRITE_BOTH_READ_SECOND);


        TableMigrationStateMap stateMap = TableMigrationStateMap.builder()
                .putTableMigrationStateMap(tableReference1, state1)
                .build();

        Map<TableReference, TableMigrationState> toUpdate = new HashMap<>(stateMap.tableMigrationStateMap());
        toUpdate.put(tableReference1, state2);

        TableMigrationStateMap stateMap2 = TableMigrationStateMap.builder()
                .tableMigrationStateMap(toUpdate)
                .build();


        System.out.println("blah");
    }

}