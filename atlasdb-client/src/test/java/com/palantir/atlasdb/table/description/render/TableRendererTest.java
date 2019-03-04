/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.table.description.render;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.IndexMetadata;
import com.palantir.atlasdb.table.description.OptionalType;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;

public class TableRendererTest {

    private static TableReference TABLE_REF = TableReference.createWithEmptyNamespace("TestTable");
    private static SortedSet<IndexMetadata> NO_INDICES = new TreeSet<>();

    @Test
    public void testCanRenderGuavaOptionals() {
        TableRenderer renderer = new TableRenderer("package", Namespace.DEFAULT_NAMESPACE, OptionalType.GUAVA);
        assertThat(renderer.render("table", getSimpleTableDefinition(TABLE_REF), NO_INDICES),
                allOf(
                        containsString("import com.google.common.base.Optional"),
                        not(containsString("import java.util.Optional")),
                        containsString("{@link Optional}"),
                        containsString("Optional<TestTableRowResult> getRow("),
                        containsString("Optional.absent"),
                        not(containsString("Optional.empty"))));
    }

    @Test
    public void testCanRenderJava8Optionals() {
        TableRenderer renderer = new TableRenderer("package", Namespace.DEFAULT_NAMESPACE, OptionalType.JAVA8);
        assertThat(renderer.render("table", getSimpleTableDefinition(TABLE_REF), NO_INDICES),
                allOf(
                        not(containsString("import com.google.common.base.Optional")),
                        containsString("import java.util.Optional"),
                        containsString("{@link Optional}"),
                        containsString("Optional<TestTableRowResult> getRow("),
                        not(containsString("Optional.absent")),
                        containsString("Optional.empty")));
    }

    private TableDefinition getSimpleTableDefinition(TableReference tableRef) {
        return new TableDefinition() {{
            javaTableName(tableRef.getTablename());
            rowName();
            rowComponent("rowName", ValueType.STRING);
            columns();
            column("col1", "1", ValueType.VAR_LONG);
        }};
    }
}
