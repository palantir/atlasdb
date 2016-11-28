/**
 * Copyright 2016 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.cassandra.multinode;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

//import com.google.common.collect.ImmutableMap;
//import com.google.common.collect.ImmutableSet;
//import com.palantir.atlasdb.AtlasDbConstants;
//import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.exception.PalantirRuntimeException;

public class OneNodeDownTableManipulationTest {
//    private static final TableReference NEW_TABLE = TableReference.createWithEmptyNamespace("new_table");
//    private static final TableReference NEW_TABLE2 = TableReference.createWithEmptyNamespace("new_table2");

    @Rule
    public ExpectedException expectException = ExpectedException.none();

//    @Test
//    public void createTableThrows() {
//        expectException.expect(PalantirRuntimeException.class);
//        OneNodeDownTestSuite.db.createTable(NEW_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
//    }
//
//    @Test
//    public void createTablesThrows() {
//        expectException.expect(PalantirRuntimeException.class);
//        OneNodeDownTestSuite.db.createTables(ImmutableMap.of(NEW_TABLE2, AtlasDbConstants.GENERIC_TABLE_METADATA));
//    }
//
//    @Test
//    public void dropTableThrows() {
//        expectException.expect(PalantirRuntimeException.class);
//        OneNodeDownTestSuite.db.dropTable(OneNodeDownTestSuite.TEST_TABLE);
//    }
//
//    @Test
//    public void dropTablesThrows() {
//        expectException.expect(PalantirRuntimeException.class);
//        OneNodeDownTestSuite.db.dropTables(ImmutableSet.of(OneNodeDownTestSuite.TEST_TABLE));
//    }

    @Test
    public void canCompactInternally() {
        OneNodeDownTestSuite.db.compactInternally(OneNodeDownTestSuite.TEST_TABLE);
    }

    @Test
    public void canCleanUpSchemaMutationLockTablesState() throws Exception {
        OneNodeDownTestSuite.db.cleanUpSchemaMutationLockTablesState();
    }

    @Test
    public void truncateTableThrows() {
        expectException.expect(PalantirRuntimeException.class);
        OneNodeDownTestSuite.db.truncateTable(OneNodeDownTestSuite.TEST_TABLE);
    }
}
