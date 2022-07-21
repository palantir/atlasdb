/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assume.assumeTrue;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.Schemas;
import java.util.regex.Pattern;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TableNameTest {
    private static final Pattern PERIOD_REGEX = Pattern.compile("\\.");

    private final Namespace namespace;
    private final String tableName;
    private final String internalTableName;

    @Parameterized.Parameters(name = "namespace={0}, tableName={1} = internalTableName={2}")
    public static Object[] data() {
        return new Object[] {
            new Object[] {Namespace.DEFAULT_NAMESPACE, "_hidden", "default___hidden"},
            new Object[] {Namespace.DEFAULT_NAMESPACE, "foo_bar", "default__foo_bar"},
            new Object[] {Namespace.DEFAULT_NAMESPACE, "test", "default__test"},
            new Object[] {Namespace.DEFAULT_NAMESPACE, "", "default__"},
            new Object[] {Namespace.EMPTY_NAMESPACE, "_hidden", "_hidden"},
            new Object[] {Namespace.EMPTY_NAMESPACE, "test", "test"},
            new Object[] {Namespace.EMPTY_NAMESPACE, "foo_bar", "foo_bar"},
            new Object[] {Namespace.EMPTY_NAMESPACE, "", ""},
            new Object[] {Namespace.create("ns"), "_hidden", "ns___hidden"},
            new Object[] {Namespace.create("ns"), "foo_bar", "ns__foo_bar"},
            new Object[] {Namespace.create("ns"), "test", "ns__test"},
            new Object[] {Namespace.create("ns"), "", "ns__"},
        };
    }

    public TableNameTest(Namespace namespace, String tableName, String internalTableName) {
        this.namespace = namespace;
        this.tableName = tableName;
        this.internalTableName = internalTableName;
    }

    @Test
    public void roundTrip() {
        assumeTrue("Table name must be valid: '" + tableName + "'", Schemas.isTableNameValid(tableName));
        checkValidTableName(tableName);
        TableReference tableRef = TableReference.create(namespace, tableName);
        assertThat(AbstractKeyValueService.internalTableName(tableRef))
                .isEqualTo(internalTableName)
                .isEqualTo(inefficientLegacyInternalTableName(tableRef));
        TableReference fromInternalTableName = TableReference.fromInternalTableName(internalTableName);
        assertThat(fromInternalTableName.getNamespace()).isEqualTo(namespace);
        assertThat(fromInternalTableName.getTableName())
                .isEqualTo(tableName)
                .satisfies(TableNameTest::checkValidTableName);
    }

    private static void checkValidTableName(String tableName) {
        assertThat(Schemas.isTableNameValid(tableName))
                .describedAs("Invalid table name '%s'", tableName)
                .isTrue();
    }

    private static String inefficientLegacyInternalTableName(TableReference tableRef) {
        String tableName = tableRef.getQualifiedName();
        if (tableName.startsWith("_")) {
            return tableName;
        }
        return PERIOD_REGEX.matcher(tableName).replaceFirst("__");
    }
}
