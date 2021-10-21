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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.Function;
import com.google.common.collect.Ordering;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.persister.JsonNodePersister;
import com.palantir.atlasdb.table.description.IndexMetadata;
import com.palantir.atlasdb.table.description.OptionalType;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import java.util.SortedSet;
import java.util.TreeSet;
import org.junit.Test;

public class TableRendererTest {

    private static TableReference TABLE_REF = TableReference.createWithEmptyNamespace("TestTable");
    private static SortedSet<IndexMetadata> NO_INDICES =
            new TreeSet<>(Ordering.natural().onResultOf((Function<IndexMetadata, String>) IndexMetadata::getIndexName));

    @Test
    public void testCanRenderGuavaOptionals() {
        TableRenderer renderer = new TableRenderer("package", Namespace.DEFAULT_NAMESPACE, OptionalType.GUAVA);
        assertThat(renderer.render("table", getSimpleTableDefinition(TABLE_REF), NO_INDICES))
                .contains("import com.google.common.base.Optional")
                .doesNotContain("import java.util.Optional")
                .contains("{@link Optional}")
                .contains("Optional<TestTableRowResult> getRow(")
                .contains("Optional.absent")
                .doesNotContain("Optional.empty");
    }

    @Test
    public void testCanRenderJava8Optionals() {
        TableRenderer renderer = new TableRenderer("package", Namespace.DEFAULT_NAMESPACE, OptionalType.JAVA8);
        assertThat(renderer.render("table", getSimpleTableDefinition(TABLE_REF), NO_INDICES))
                .doesNotContain("import com.google.common.base.Optional")
                .contains("import java.util.Optional")
                .contains("{@link Optional}")
                .contains("Optional<TestTableRowResult> getRow(")
                .doesNotContain("Optional.absent")
                .contains("Optional.empty");
    }

    private TableDefinition getSimpleTableDefinition(TableReference tableRef) {
        return new TableDefinition() {
            {
                javaTableName(tableRef.getTableName());
                rowName();
                rowComponent("rowName", ValueType.STRING);
                columns();
                column("col1", "1", ValueType.VAR_LONG);
            }
        };
    }

    @Test
    public void testReusablePersisters() {
        TableRenderer renderer = new TableRenderer("package", Namespace.DEFAULT_NAMESPACE, OptionalType.JAVA8);
        String renderedTableDefinition =
                renderer.render("table", getTableWithUserSpecifiedPersister(TABLE_REF), NO_INDICES);
        assertThat(renderedTableDefinition)
                .contains("REUSABLE_PERSISTER.hydrateFromBytes")
                .contains("private final com.palantir.atlasdb.persister.JsonNodePersister REUSABLE_PERSISTER =");
    }

    private TableDefinition getTableWithUserSpecifiedPersister(TableReference tableRef) {
        return new TableDefinition() {
            {
                javaTableName(tableRef.getTableName());
                rowName();
                rowComponent("rowName", ValueType.STRING);
                columns();
                column("col1", "1", JsonNodePersister.class);
            }
        };
    }

    @Test
    public void testReusablePersistersInDynamicColumns() {
        TableRenderer renderer = new TableRenderer("package", Namespace.DEFAULT_NAMESPACE, OptionalType.JAVA8);
        String renderedTableDefinition =
                renderer.render("table", getTableWithUserSpecifiedPersisterInDynamicColumns(TABLE_REF), NO_INDICES);
        assertThat(renderedTableDefinition)
                .contains("private static final com.palantir.atlasdb.persister.JsonNodePersister REUSABLE_PERSISTER =");
    }

    private TableDefinition getTableWithUserSpecifiedPersisterInDynamicColumns(TableReference tableRef) {
        return new TableDefinition() {
            {
                javaTableName(tableRef.getTableName());
                rowName();
                rowComponent("rowName", ValueType.STRING);
                dynamicColumns();
                columnComponent("colName", ValueType.STRING);
                value(JsonNodePersister.class);
            }
        };
    }
}
