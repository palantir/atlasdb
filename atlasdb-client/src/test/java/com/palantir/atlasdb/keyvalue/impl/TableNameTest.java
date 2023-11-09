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

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.Schemas;
import java.util.List;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TableNameTest {
    private static final Pattern PERIOD_REGEX = Pattern.compile("\\.");

    public static List<Arguments> getParameters() {
        return List.of(
                Arguments.of(Namespace.DEFAULT_NAMESPACE, "_hidden", "default___hidden"),
                Arguments.of(Namespace.DEFAULT_NAMESPACE, "foo_bar", "default__foo_bar"),
                Arguments.of(Namespace.DEFAULT_NAMESPACE, "test", "default__test"),
                Arguments.of(Namespace.DEFAULT_NAMESPACE, "", "default__"),
                Arguments.of(Namespace.EMPTY_NAMESPACE, "_hidden", "_hidden"),
                Arguments.of(Namespace.EMPTY_NAMESPACE, "test", "test"),
                Arguments.of(Namespace.EMPTY_NAMESPACE, "foo_bar", "foo_bar"),
                Arguments.of(Namespace.EMPTY_NAMESPACE, "", ""),
                Arguments.of(Namespace.create("ns"), "_hidden", "ns___hidden"),
                Arguments.of(Namespace.create("ns"), "foo_bar", "ns__foo_bar"),
                Arguments.of(Namespace.create("ns"), "test", "ns__test"),
                Arguments.of(Namespace.create("ns"), "", "ns__"));
    }

    @ParameterizedTest(name = "namespace={0}, tableName={1} = internalTableName={2}")
    @MethodSource("getParameters")
    public void roundTrip(Namespace namespace, String tableName, String internalTableName) {
        Assumptions.assumeTrue(Schemas.isTableNameValid(tableName), "Table name must be valid: '" + tableName + "'");
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
