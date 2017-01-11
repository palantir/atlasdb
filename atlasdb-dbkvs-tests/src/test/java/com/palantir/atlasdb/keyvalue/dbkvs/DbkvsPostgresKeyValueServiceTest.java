/**
 * Copyright 2015 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.dbkvs;

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueServiceTest;

public class DbkvsPostgresKeyValueServiceTest extends AbstractKeyValueServiceTest {
    @Override
    protected KeyValueService getKeyValueService() {
        KeyValueService kvs = ConnectionManagerAwareDbKvs.create(DbkvsPostgresTestSuite.getKvsConfig());
        for (TableReference table : kvs.getAllTableNames()) {
            if (!table.getQualifiedName().equals("_metadata")) {
                kvs.dropTable(table);
            }
        }
        return kvs;
    }

    @Test
    public void handlesLongTablenamesWithoutMergingThem() {
        // Create two empty tables with names only unique well past the name limit
        TableReference longTablename1 = TableReference.createFromFullyQualifiedName(
                "atlas.ThisIsAVeryLongTableNameThatWillExceedEvenTheLongerPostgresLimit");
        TableReference longTablename2 = TableReference.createFromFullyQualifiedName(
                "atlas.ThisIsAVeryLongTableNameThatWillExceedEvenTheLongerPostgresLimitAsWell");
        keyValueService.createTables(ImmutableMap.of(
                longTablename1, AtlasDbConstants.GENERIC_TABLE_METADATA,
                longTablename2, AtlasDbConstants.GENERIC_TABLE_METADATA));
        keyValueService.truncateTables(ImmutableSet.of(longTablename1, longTablename2));

        // Put some data in table 1
        Cell key = Cell.create("foo".getBytes(StandardCharsets.UTF_8), "bar".getBytes(StandardCharsets.UTF_8));
        byte[] value = "baz".getBytes(StandardCharsets.UTF_8);
        keyValueService.put(longTablename1, ImmutableMap.of(key, value), 0L);

        // Try and retrieve the data we put in table 1 from table 2
        Map<Cell, Value> cellValueMap = keyValueService.get(longTablename2, ImmutableMap.of(key, Long.MAX_VALUE));

        // If the data is present in table 2, the tables have merged
        assertEquals(cellValueMap, ImmutableMap.of());

        keyValueService.dropTables(ImmutableSet.of(longTablename1, longTablename2));
    }
}
