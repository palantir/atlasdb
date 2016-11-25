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

import static com.palantir.cassandra.multinode.OneNodeDownTestSuite.TEST_TABLE;
import static com.palantir.cassandra.multinode.OneNodeDownTestSuite.db;

import java.lang.reflect.InvocationTargetException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.exception.PalantirRuntimeException;

public class OneNodeDownTableManipulationTest {
    private static final TableReference NEW_TABLE = TableReference.createWithEmptyNamespace("new_table");

    @Rule
    public ExpectedException expect_exception = ExpectedException.none();

    @Test
    public void createTableThrowsISE(){
        expect_exception.expect(IllegalStateException.class);
        db.createTable(NEW_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
    }

    @Test
    public void dropTableThrows(){
        expect_exception.expect(StackOverflowError.class);
        db.dropTable(TEST_TABLE);
    }

    @Test
    public void canCompactInternally(){
        db.compactInternally(TEST_TABLE);
    }

    @Test
    public void canCleanUpSchemaMutationLockTablesState() throws Exception {
        db.cleanUpSchemaMutationLockTablesState();
    }

    @Test
    public void truncateTableThrowsPRE(){
        expect_exception.expect(PalantirRuntimeException.class);
        db.truncateTable(TEST_TABLE);
    }
}
