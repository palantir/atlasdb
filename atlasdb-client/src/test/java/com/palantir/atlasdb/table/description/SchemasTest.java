/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.table.description;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.schema.Namespace;
import com.palantir.atlasdb.schema.SweepSchema;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public class SchemasTest {
    private static String TABLE_NAME = "testTable";
    private static Namespace NAMESPACE = Namespace.create("testNamespace");
    Mockery mockery;
    KeyValueService kvs;

    @Before
    public void setup() {
        mockery = new Mockery();
        kvs = mockery.mock(KeyValueService.class);
    }

    @Test
    public void testGetFullTableName() {
        MatcherAssert.assertThat(
                Schemas.getFullTableName(TABLE_NAME, NAMESPACE),
                Matchers.equalTo(NAMESPACE.getName() + "." + TABLE_NAME));
    }

    @Test
    public void testGetFullTableNameLegacy() {
        MatcherAssert.assertThat(
                Schemas.getFullTableName(TABLE_NAME, Namespace.create("met")),
                Matchers.equalTo(TABLE_NAME)
        );
    }

    @Test
    public void testGetFullTableNameEmptyNamespace() {
        MatcherAssert.assertThat(
                Schemas.getFullTableName(TABLE_NAME, Namespace.EMPTY_NAMESPACE),
                Matchers.equalTo(TABLE_NAME)
        );
    }

    @Test
    public void testCreateTable() {
        mockery.checking(new Expectations(){{
            oneOf(kvs).createTables(with(tableMapContainsEntry(TABLE_NAME, getSimpleTableDefinitionAsBytes(TABLE_NAME))));
        }});
        Schemas.createTable(kvs, TABLE_NAME, getSimpleTableDefinition(TABLE_NAME));
    }

    @Test
    public void testCreateTables() {
        String tableName1 = TABLE_NAME + "1";
        String tableName2 = TABLE_NAME + "2";
        mockery.checking(new Expectations(){{
            oneOf(kvs).createTables(with(tableMapContainsEntry(tableName1, getSimpleTableDefinitionAsBytes(tableName1))));
            oneOf(kvs).createTables(with(tableMapContainsEntry(tableName2, getSimpleTableDefinitionAsBytes(tableName2))));
        }});
        Map<String, TableDefinition> tables = Maps.newHashMap();
        tables.put(tableName1, getSimpleTableDefinition(tableName1));
        tables.put(tableName2, getSimpleTableDefinition(tableName2));
        Schemas.createTables(kvs, tables);
    }

    @Test
    public void testDeleteTable() {
        mockery.checking(new Expectations(){{
            oneOf(kvs).dropTable(with(equal(TABLE_NAME)));
        }});
        Schemas.deleteTable(kvs, TABLE_NAME);
    }

    @Test
    public void testDeleteTablesForSweepSchema() {
        Set<String> allTableNames = Sets.newHashSet();
        allTableNames.add("sweep.progress");
        allTableNames.add("sweep.priority");

        mockery.checking(new Expectations(){{
            oneOf(kvs).getAllTableNames(); will(returnValue(allTableNames));
            oneOf(kvs).dropTables(allTableNames);
            oneOf(kvs).getAllTableNames();
        }});
        Schemas.deleteTablesAndIndexes(SweepSchema.INSTANCE.getLatestSchema(), kvs);
    }

    private Matcher<Map<String, byte[]>> tableMapContainsEntry(String tableName, byte[] description) {
        return new TypeSafeDiagnosingMatcher<Map<String, byte[]>>() {
            @Override
            protected boolean matchesSafely(Map<String, byte[]> item, Description mismatchDescription) {
                mismatchDescription.appendText("Map does not contain match: ").appendValue(tableName);
                return item.containsKey(tableName) && Arrays.equals(description, item.get(tableName));
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("Contains key: ").appendValue(tableName);
            }
        };
    }

    private TableDefinition getSimpleTableDefinition(String tableName) {
        return new TableDefinition() {{
            javaTableName(tableName);
            rowName();
                rowComponent("rowName", ValueType.STRING);
            columns();
                column("col1", "1", ValueType.VAR_LONG);
                column("col2", "2", ValueType.VAR_LONG);
            conflictHandler(ConflictHandler.IGNORE_ALL);
        }};
    }

    private byte[] getSimpleTableDefinitionAsBytes(String tableName) {
        return getSimpleTableDefinition(tableName).toTableMetadata().persistToBytes();
    }

    private Schema getMockSchema() {
        Schema singleTableSchema = mockery.mock(Schema.class);
        mockery.checking(new Expectations() {{
            oneOf(singleTableSchema).getTableDefinition(TABLE_NAME); will(returnValue(getSimpleTableDefinition(TABLE_NAME)));
        }});

        return singleTableSchema;
    }
}

