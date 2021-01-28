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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.UnsignedBytes;
import com.google.protobuf.InvalidProtocolBufferException;
import com.palantir.atlasdb.compress.CompressionUtils;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelections;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.Prefix;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.table.api.AtlasDbDynamicMutablePersistentTable;
import com.palantir.atlasdb.table.api.AtlasDbMutablePersistentTable;
import com.palantir.atlasdb.table.api.AtlasDbNamedMutableTable;
import com.palantir.atlasdb.table.api.AtlasDbNamedPersistentSet;
import com.palantir.atlasdb.table.api.ColumnValue;
import com.palantir.atlasdb.table.api.TypedRowResult;
import com.palantir.atlasdb.table.description.ColumnValueDescription.Compression;
import com.palantir.atlasdb.table.description.IndexComponent;
import com.palantir.atlasdb.table.description.IndexDefinition.IndexType;
import com.palantir.atlasdb.table.description.IndexMetadata;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.OptionalType;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.table.generation.ColumnValues;
import com.palantir.atlasdb.table.generation.Descending;
import com.palantir.atlasdb.table.generation.NamedColumnValue;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConstraintCheckingTransaction;
import com.palantir.atlasdb.transaction.api.ImmutableGetRangesQuery;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.common.base.AbortingVisitor;
import com.palantir.common.base.AbortingVisitors;
import com.palantir.common.base.BatchingVisitable;
import com.palantir.common.base.BatchingVisitableView;
import com.palantir.common.base.BatchingVisitables;
import com.palantir.common.base.Throwables;
import com.palantir.common.collect.IterableView;
import com.palantir.common.persist.Persistable;
import com.palantir.common.persist.Persistable.Hydrator;
import com.palantir.common.persist.Persistables;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.util.AssertUtils;
import com.palantir.util.crypto.Sha256Hash;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Generated;
import javax.annotation.Nullable;

@SuppressWarnings("checkstyle:all") // too many warnings to fix
public class TableRenderer {
    private final String packageName;
    private final Namespace namespace;
    private final OptionalType optionalType;

    public TableRenderer(String packageName, Namespace namespace, OptionalType optionalType) {
        this.packageName = com.palantir.logsafe.Preconditions.checkNotNull(packageName);
        this.namespace = com.palantir.logsafe.Preconditions.checkNotNull(namespace);
        this.optionalType = com.palantir.logsafe.Preconditions.checkNotNull(optionalType, "Must specify optionalType");
    }

    public String getClassName(String rawTableName, TableDefinition table) {
        return Renderers.getClassTableName(rawTableName, table) + "Table";
    }

    public String render(String rawTableName, TableDefinition table, SortedSet<IndexMetadata> indices) {
        return new ClassRenderer(rawTableName, table, indices).render();
    }

    private class ClassRenderer extends Renderer {
        private final String tableName;
        private final TableMetadata table;
        private final SortedSet<IndexMetadata> indices;
        private final Collection<IndexMetadata> cellReferencingIndices;
        private final String raw_table_name;
        private final boolean isGeneric;
        private final boolean isNestedIndex;
        private final String outerTable;
        private final String Table;
        private final String Row;
        private final String Column;
        private final String ColumnValue;
        private final String RowResult;
        private final String Trigger;

        public ClassRenderer(String rawTableName, TableDefinition table, SortedSet<IndexMetadata> indices) {
            Preconditions.checkArgument(Schemas.isTableNameValid(rawTableName), "Invalid table name %s", rawTableName);
            this.tableName = Renderers.getClassTableName(rawTableName, table);
            this.table = table.toTableMetadata();
            this.indices = indices;
            this.cellReferencingIndices = getCellReferencingIndices(indices);
            this.raw_table_name = rawTableName;
            this.isGeneric = table.getGenericTableName() != null;
            this.isNestedIndex = false;
            this.outerTable = null;
            this.Table = tableName + "Table";
            this.Row = tableName + "Row";
            this.Column = tableName + (isDynamic(this.table) ? "Column" : "NamedColumn");
            this.ColumnValue = tableName + (isDynamic(this.table) ? "ColumnValue" : "NamedColumnValue<?>");
            this.RowResult = tableName + "RowResult";
            this.Trigger = tableName + "Trigger";
        }

        public ClassRenderer(Renderer parent, String outerTable, IndexMetadata index) {
            super(parent);
            this.tableName = Renderers.getIndexTableName(index);
            this.table = index.getTableMetadata();
            this.indices = ImmutableSortedSet.of();
            this.cellReferencingIndices = ImmutableList.of();
            this.raw_table_name = index.getIndexName();
            this.isGeneric = false;
            this.isNestedIndex = true;
            this.outerTable = outerTable;
            this.Table = tableName + "Table";
            this.Row = tableName + "Row";
            this.Column = tableName + (isDynamic(this.table) ? "Column" : "NamedColumn");
            this.ColumnValue = tableName + (isDynamic(this.table) ? "ColumnValue" : "NamedColumnValue<?>");
            this.RowResult = tableName + "RowResult";
            this.Trigger = tableName + "Trigger";
        }

        @Override
        protected void run() {
            ImportRenderer importRenderer = new ImportRenderer(this, getImports(optionalType));
            if (!isNestedIndex) {
                line("package ", packageName, ";");
                line();
                importRenderer.renderImports();
            }
            line("@Generated(\"", TableRenderer.class.getName(), "\")");
            line("@SuppressWarnings({\"all\", \"deprecation\"})");
            line("public ", isNestedIndex ? "static " : "", "final class ", Table, " implements");
            if (isNamedSet(table)) {
                line("        AtlasDbNamedPersistentSet<", Table, ".", Row, ">,");
            }
            if (isDynamic(table)) {
                line("        AtlasDbDynamicMutablePersistentTable<", Table, ".", Row, ",");
                line("                                                ", Table, ".", Column, ",");
                line("                                                ", Table, ".", ColumnValue, ",");
                line("                                                ", Table, ".", RowResult, "> {");
            } else {
                line("        AtlasDbMutablePersistentTable<", Table, ".", Row, ",");
                line("                                         ", Table, ".", ColumnValue, ",");
                line("                                         ", Table, ".", RowResult, ">,");
                line("        AtlasDbNamedMutableTable<", Table, ".", Row, ",");
                line("                                    ", Table, ".", ColumnValue, ",");
                line("                                    ", Table, ".", RowResult, "> {");
            }
            {
                fields(isDynamic(table));
                line();
                staticFactories();
                line();
                constructors();
                if (!isGeneric) {
                    line();
                    renderGetRawTableName();
                }
                line();
                renderGetTableRef();
                line();
                renderGetTableName();
                line();
                renderGetNamespace();
                line();
                new RowOrDynamicColumnRenderer(this, Row, table.getRowMetadata(), table.isRangeScanAllowed(), false)
                        .run();
                line();
                if (isDynamic(table)) {
                    renderDynamic();
                } else {
                    renderNamed();
                }
                line();
                if (table.isRangeScanAllowed()) {
                    renderOptimizeRangeRequests();
                    line();
                    renderGetRange();
                    line();
                    renderGetRanges();
                    line();
                    renderDeleteRange();
                    line();
                    if (isDynamic(table)) {
                        renderDynamicDeleteRanges();
                    } else {
                        renderNamedDeleteRanges();
                    }
                } else {
                    renderOptimizeColumnSelection();
                    line();
                    renderGetAllRowsUnordered();
                }
                line();
                if (isNamedSet(table)) {
                    renderAdd();
                    line();
                }
                renderFindConstraintFailures();
                for (IndexMetadata index : indices) {
                    line();
                    new ClassRenderer(this, Table, index).run();
                }
                if (!isNestedIndex) {
                    line();
                    importRenderer.renderImportJavaDoc();
                    renderClassHash();
                }
            }
            line("}");
            line();
        }

        private void renderNamed() {
            line("public interface ", tableName, "NamedColumnValue<T> extends NamedColumnValue<T> { /* */ }");
            line();
            for (NamedColumnDescription col : ColumnRenderers.namedColumns(table)) {
                new NamedColumnValueRenderer(this, tableName, col).run();
                line();
            }
            renderTrigger();
            line();
            new NamedRowResultRenderer(this, tableName, ColumnRenderers.namedColumns(table)).run();
            line();
            new NamedColumnRenderer(this, tableName, ColumnRenderers.namedColumns(table)).run();
            line();
            renderColumnSelection(false);
            line();
            renderShortNameToHydrator();
            line();
            for (NamedColumnDescription col : table.getColumns().getNamedColumns()) {
                renderNamedGetColumn(col);
                line();
            }
            for (NamedColumnDescription col : table.getColumns().getNamedColumns()) {
                renderNamedPutColumn(col);
                line();
            }
            renderNamedPut();
            line();
            for (NamedColumnDescription col : table.getColumns().getNamedColumns()) {
                renderNamedDeleteColumn(col);
                line();
            }
            renderNamedDelete();
            line();
            renderNamedGetRow();
            line();
            renderNamedGetRows();
            line();
            renderGetRowColumns(false);
            line();
            renderGetRowsMultimap(false);
            line();
            renderGetRowsColumnRange(false);
            line();
            renderGetRowsColumnRangeIterator(false);

            if (!cellReferencingIndices.isEmpty()) {
                line();
                renderNamedGetAffectedCells();
                for (IndexMetadata index : cellReferencingIndices) {
                    line();
                    renderCellReferencingIndexDelete(index);
                }
            }
        }

        private void renderDynamic() {
            new RowOrDynamicColumnRenderer(
                            this, Column, table.getColumns().getDynamicColumn().getColumnNameDesc(), false, true)
                    .run();
            line();
            renderTrigger();
            line();
            new DynamicColumnValueRenderer(this, tableName, table.getColumns().getDynamicColumn()).run();
            line();
            new DynamicRowResultRenderer(
                            this,
                            tableName,
                            table.getColumns().getDynamicColumn().getValue())
                    .run();
            line();
            renderDynamicDelete();
            line();
            renderDynamicPut();
            line();
            renderDynamicTouch();
            line();
            renderColumnSelection(true);
            line();
            renderDynamicGet();
            line();
            renderGetRowColumns(true);
            line();
            renderGetRowsMultimap(true);
            line();
            renderGetRowsColumnRange(true);
            line();
            renderGetRowsColumnRangeIterator(true);
        }

        private void fields(boolean isDynamic) {
            line("private final Transaction t;");
            line("private final List<", Trigger, "> triggers;");
            if (!isGeneric) {
                line("private final static String rawTableName = \"" + raw_table_name + "\";");
            }
            line("private final TableReference tableRef;");
            line(
                    "private final static ColumnSelection allColumns = ",
                    isDynamic ? "ColumnSelection.all();" : "getColumnSelection(" + Column + ".values());");
        }

        private void staticFactories() {
            if (isNestedIndex) {
                line(
                        "public static ",
                        Table,
                        " of(",
                        outerTable,
                        " table",
                        isGeneric ? ", String tableName" : "",
                        ") {");
                {
                    line(
                            "return new ",
                            Table,
                            "(table.t, table.tableRef.getNamespace()",
                            isGeneric ? ", tableName" : "",
                            ", ImmutableList.<",
                            Trigger,
                            ">of());");
                }
                line("}");
                line();
                line(
                        "public static ",
                        Table,
                        " of(",
                        outerTable,
                        " table",
                        isGeneric ? ", String tableName" : "",
                        ", ",
                        Trigger,
                        " trigger, ",
                        Trigger,
                        "... triggers) {");
                {
                    line(
                            "return new ",
                            Table,
                            "(table.t, table.tableRef.getNamespace()",
                            isGeneric ? ", tableName" : "",
                            ", ImmutableList.<",
                            Trigger,
                            ">builder().add(trigger).add(triggers).build());");
                }
                line("}");
                line();
                line(
                        "public static ",
                        Table,
                        " of(",
                        outerTable,
                        " table",
                        isGeneric ? ", String tableName" : "",
                        ", List<",
                        Trigger,
                        "> triggers) {");
                {
                    line(
                            "return new ",
                            Table,
                            "(table.t, table.tableRef.getNamespace()",
                            isGeneric ? ", tableName" : "",
                            ", triggers);");
                }
                line("}");
            } else {
                line(
                        "static ",
                        Table,
                        " of(Transaction t, Namespace namespace",
                        isGeneric ? ", String tableName" : "",
                        ") {");
                {
                    line(
                            "return new ",
                            Table,
                            "(t, namespace",
                            isGeneric ? ", tableName" : "",
                            ", ImmutableList.<",
                            Trigger,
                            ">of());");
                }
                line("}");
                line();
                line(
                        "static ",
                        Table,
                        " of(Transaction t, Namespace namespace",
                        isGeneric ? ", String tableName" : "",
                        ", ",
                        Trigger,
                        " trigger, ",
                        Trigger,
                        "... triggers) {");
                {
                    line(
                            "return new ",
                            Table,
                            "(t, namespace",
                            isGeneric ? ", tableName" : "",
                            ", ImmutableList.<",
                            Trigger,
                            ">builder().add(trigger).add(triggers).build());");
                }
                line("}");
                line();
                line(
                        "static ",
                        Table,
                        " of(Transaction t, Namespace namespace",
                        isGeneric ? ", String tableName" : "",
                        ", List<",
                        Trigger,
                        "> triggers) {");
                {
                    line("return new ", Table, "(t, namespace", isGeneric ? ", tableName" : "", ", triggers);");
                }
                line("}");
            }
        }

        private void constructors() {
            line(
                    "private ",
                    Table,
                    "(Transaction t, Namespace namespace",
                    isGeneric ? ", String tableName" : "",
                    ", List<",
                    Trigger,
                    "> triggers) {");
            {
                line("this.t = t;");
                if (isGeneric) {
                    line("this.tableRef = TableReference.create(namespace, tableName);");
                } else {
                    line("this.tableRef = TableReference.create(namespace, rawTableName);");
                }
                line("this.triggers = triggers;");
            }
            line("}");
        }

        private void renderGetRawTableName() {
            line("public static String getRawTableName() {");
            {
                line("return rawTableName;");
            }
            line("}");
        }

        private void renderGetTableRef() {
            line("public TableReference getTableRef() {");
            {
                line("return tableRef;");
            }
            line("}");
        }

        private void renderGetTableName() {
            line("public String getTableName() {");
            {
                line("return tableRef.getQualifiedName();");
            }
            line("}");
        }

        private void renderGetNamespace() {
            line("public Namespace getNamespace() {");
            {
                line("return tableRef.getNamespace();");
            }
            line("}");
        }

        private void renderTrigger() {
            line("public interface ", Trigger, " {");
            {
                line("public void put", tableName, "(Multimap<", Row, ", ? extends ", ColumnValue, "> newRows);");
            }
            line("}");
        }

        private void renderDynamicDelete() {
            line("@Override");
            line("public void delete(", Row, " row, ", Column, " column) {");
            {
                line("delete(ImmutableMultimap.of(row, column));");
            }
            line("}");
            line();
            line("@Override");
            line("public void delete(Iterable<", Row, "> rows) {");
            {
                line("Multimap<", Row, ", ", Column, "> toRemove = HashMultimap.create();");
                line("Multimap<", Row, ", ", ColumnValue, "> result = getRowsMultimap(rows);");
                line("for (Entry<", Row, ", ", ColumnValue, "> e : result.entries()) {");
                {
                    line("toRemove.put(e.getKey(), e.getValue().getColumnName());");
                }
                line("}");
                line("delete(toRemove);");
            }
            line("}");
            line();
            line("@Override");
            line("public void delete(Multimap<", Row, ", ", Column, "> values) {");
            {
                line("t.delete(tableRef, ColumnValues.toCells(values));");
            }
            line("}");
        }

        private void renderDynamicPut() {
            line("@Override");
            line("public void put(", Row, " rowName, Iterable<", ColumnValue, "> values) {");
            {
                line("put(ImmutableMultimap.<", Row, ", ", ColumnValue, ">builder().putAll(rowName, values).build());");
            }
            line("}");
            line();
            line("@Override");
            line("public void put(", Row, " rowName, ", ColumnValue, "... values) {");
            {
                line("put(ImmutableMultimap.<", Row, ", ", ColumnValue, ">builder().putAll(rowName, values).build());");
            }
            line("}");
            line();
            line("@Override");
            line("public void put(Multimap<", Row, ", ? extends ", ColumnValue, "> values) {");
            {
                line("t.useTable(tableRef, this);");
                if (!indices.isEmpty()) {
                    line("for (Entry<", Row, ", ? extends ", ColumnValue, "> e : values.entries()) {");
                    {
                        for (IndexMetadata index : indices) {
                            renderIndexPut(index);
                        }
                    }
                    line("}");
                }
                line("t.put(tableRef, ColumnValues.toCellValues(values));");
                line("for (", Trigger, " trigger : triggers) {");
                {
                    line("trigger.put", tableName, "(values);");
                }
                line("}");
            }
            line("}");
        }

        private void renderIndexPut(IndexMetadata index) {
            List<String> rowArgumentNames = new ArrayList<>();
            List<String> colArgumentNames = new ArrayList<>();
            List<TypeAndName> iterableArgNames = new ArrayList<>();
            boolean hasCol = index.getColumnNameToAccessData() != null;
            String indexName = Renderers.getIndexTableName(index);
            String columnClass = null;
            if (hasCol) {
                columnClass = Renderers.CamelCase(index.getColumnNameToAccessData());
                line("if (e.getValue() instanceof ", columnClass, ")");
            } else if (isDynamic(table)) {
                columnClass = tableName + "ColumnValue";
            }
            line("{");
            {
                if (hasCol || isDynamic(table)) {
                    line(columnClass, " col = (", columnClass, ") e.getValue();");
                }
                if (index.getIndexCondition() != null) {
                    line("if (" + index.getIndexCondition().getValueCode("col.getValue()") + ")");
                }
                line("{");
                {
                    line(Row, " row = e.getKey();");
                    line(indexName, "Table table = ", indexName, "Table.of(this);");
                    for (IndexComponent component : index.getRowComponents()) {
                        String varName = renderIndexComponent(component);
                        rowArgumentNames.add(varName);
                        if (component.isMultiple()) {
                            iterableArgNames.add(new TypeAndName(
                                    component.getRowKeyDescription().getType().getJavaClassName(), varName));
                        }
                    }

                    if (index.getIndexType().equals(IndexType.CELL_REFERENCING)) {
                        Collections.addAll(
                                colArgumentNames, "row.persistToBytes()", "e.getValue().persistColumnName()");
                    }

                    for (IndexComponent component : index.getColumnComponents()) {
                        String varName = renderIndexComponent(component);
                        colArgumentNames.add(varName);
                        if (component.isMultiple()) {
                            iterableArgNames.add(new TypeAndName(
                                    component.getRowKeyDescription().getType().getJavaClassName(), varName));
                        }
                    }

                    for (TypeAndName iterableArg : iterableArgNames) {
                        line("for (", iterableArg.toString(), " : ", iterableArg.name, "Iterable) {");
                    }

                    line(
                            indexName,
                            "Table.",
                            indexName,
                            "Row indexRow = ",
                            indexName,
                            "Table.",
                            indexName,
                            "Row.of(",
                            Joiner.on(", ").join(rowArgumentNames),
                            ");");
                    if (!index.isDynamicIndex() && !index.getIndexType().equals(IndexType.CELL_REFERENCING)) {
                        line("table.putExists(indexRow, 0L);");
                    } else {
                        line(
                                indexName,
                                "Table.",
                                indexName,
                                "Column indexCol = ",
                                indexName,
                                "Table.",
                                indexName,
                                "Column.of(",
                                Joiner.on(", ").join(colArgumentNames),
                                ");");
                        line(
                                indexName,
                                "Table.",
                                indexName,
                                "ColumnValue indexColVal = ",
                                indexName,
                                "Table.",
                                indexName,
                                "ColumnValue.of(indexCol, 0L);");
                        line("table.put(indexRow, indexColVal);");
                    }

                    for (int i = 0; i < iterableArgNames.size(); i++) {
                        line("}");
                    }
                }
                line("}");
            }
            line("}");
        }

        private String renderIndexComponent(IndexComponent component) {
            NameComponentDescription compDesc = component.getRowKeyDescription();
            String columnCode = isDynamic(table) ? "col.getColumnName()" : null;
            String valueCode = component.getValueCode("row", columnCode, "col.getValue()");
            String varName = Renderers.camelCase(compDesc.getComponentName());
            if (component.isMultiple()) {
                line(
                        "Iterable<",
                        compDesc.getType().getJavaObjectClassName(),
                        "> ",
                        varName,
                        "Iterable = ",
                        valueCode,
                        ";");
            } else {
                line(compDesc.getType().getJavaClassName(), " ", varName, " = ", valueCode, ";");
            }
            return varName;
        }

        private void renderDynamicTouch() {
            line("@Override");
            line("public void touch(Multimap<", Row, ", ", Column, "> values) {");
            {
                line("Multimap<", Row, ", ", ColumnValue, "> currentValues = get(values);");
                line("put(currentValues);");
                line("Multimap<", Row, ", ", Column, "> toDelete = HashMultimap.create(values);");
                line("for (Map.Entry<", Row, ", ", ColumnValue, "> e : currentValues.entries()) {");
                {
                    line("toDelete.remove(e.getKey(), e.getValue().getColumnName());");
                }
                line("}");
                line("delete(toDelete);");
            }
            line("}");
        }

        private void renderGetRowColumns(boolean isDynamic) {
            line("@Override");
            line("public List<", ColumnValue, "> getRowColumns(", Row, " row) {");
            {
                line("return getRowColumns(row, allColumns);");
            }
            line("}");
            line();
            line("@Override");
            line("public List<", ColumnValue, "> getRowColumns(", Row, " row, ColumnSelection columns) {");
            {
                line("byte[] bytes = row.persistToBytes();");
                line("RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);");
                line("if (rowResult == null) {");
                {
                    line("return ImmutableList.of();");
                }
                line("} else {");
                {
                    line(
                            "List<",
                            ColumnValue,
                            "> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());");
                    line("for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {");
                    {
                        if (isDynamic) {
                            line(Column, " col = ", Column, ".BYTES_HYDRATOR.hydrateFromBytes(e.getKey());");
                            line(
                                    table.getColumns()
                                            .getDynamicColumn()
                                            .getValue()
                                            .getJavaObjectTypeName(),
                                    " val = ",
                                    ColumnValue,
                                    ".hydrateValue(e.getValue());");
                            line("ret.add(", ColumnValue, ".of(col, val));");
                        } else {
                            line(
                                    "ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));");
                        }
                    }
                    line("}");
                    line("return ret;");
                }
                line("}");
            }
            line("}");
        }

        private void renderColumnSelection(boolean isDynamic) {
            line("public static ColumnSelection getColumnSelection(Collection<", Column, "> cols) {");
            line(
                    "return ColumnSelection.create(Collections2.transform(cols, ",
                    isDynamic ? "Persistables.persistToBytesFunction()" : Column + ".toShortName()",
                    "));");
            line("}");
            line();
            line("public static ColumnSelection getColumnSelection(", Column, "... cols) {");
            line("return getColumnSelection(Arrays.asList(cols));");
            line("}");
        }

        private void renderShortNameToHydrator() {
            line("private static final Map<String, Hydrator<? extends ", ColumnValue, ">> shortNameToHydrator =");
            line("        ImmutableMap.<String, Hydrator<? extends ", ColumnValue, ">>builder()");
            for (NamedColumnDescription col : table.getColumns().getNamedColumns()) {
                line(
                        "            .put(",
                        ColumnRenderers.short_name(col),
                        ", ",
                        ColumnRenderers.VarName(col),
                        ".BYTES_HYDRATOR)");
            }
            line("            .build();");
        }

        private void renderNamedGetColumn(NamedColumnDescription col) {
            line(
                    "public Map<",
                    Row,
                    ", ",
                    ColumnRenderers.TypeName(col),
                    "> get",
                    ColumnRenderers.VarName(col),
                    "s(Collection<",
                    Row,
                    "> rows) {");
            {
                line("Map<Cell, ", Row, "> cells = Maps.newHashMapWithExpectedSize(rows.size());");
                line("for (", Row, " row : rows) {");
                {
                    line(
                            "cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes(",
                            ColumnRenderers.short_name(col),
                            ")), row);");
                }
                line("}");
                line("Map<Cell, byte[]> results = t.get(tableRef, cells.keySet());");
                line(
                        "Map<",
                        Row,
                        ", ",
                        ColumnRenderers.TypeName(col),
                        "> ret = Maps.newHashMapWithExpectedSize(results.size());");
                line("for (Entry<Cell, byte[]> e : results.entrySet()) {");
                {
                    line(
                            ColumnRenderers.TypeName(col),
                            " val = ",
                            ColumnRenderers.VarName(col),
                            ".BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();");
                    line("ret.put(cells.get(e.getKey()), val);");
                }
                line("}");
                line("return ret;");
            }
            line("}");
        }

        private void renderNamedPutColumn(NamedColumnDescription col) {
            String Value = col.getValue().getJavaObjectTypeName();
            line("public void put", ColumnRenderers.VarName(col), "(", Row, " row, ", Value, " value) {");
            {
                line("put(ImmutableMultimap.of(row, ", ColumnRenderers.VarName(col), ".of(value)));");
            }
            line("}");
            line();
            line("public void put", ColumnRenderers.VarName(col), "(Map<", Row, ", ", Value, "> map) {");
            {
                line("Map<", Row, ", ", ColumnValue, "> toPut = Maps.newHashMapWithExpectedSize(map.size());");
                line("for (Entry<", Row, ", ", Value, "> e : map.entrySet()) {");
                {
                    line("toPut.put(e.getKey(), ", ColumnRenderers.VarName(col), ".of(e.getValue()));");
                }
                line("}");
                line("put(Multimaps.forMap(toPut));");
            }
            line("}");
        }

        private void renderNamedGetAffectedCells() {
            line(
                    "private Multimap<",
                    Row,
                    ", ",
                    ColumnValue,
                    "> getAffectedCells(Multimap<",
                    Row,
                    ", ? extends ",
                    ColumnValue,
                    "> rows) {");
            {
                line("Multimap<", Row, ", ", ColumnValue, "> oldData = getRowsMultimap(rows.keySet());");
                line("Multimap<", Row, ", ", ColumnValue, "> cellsAffected = ArrayListMultimap.create();");
                line("for (", Row, " row : oldData.keySet()) {");
                {
                    line("Set<String> columns = new HashSet<String>();");
                    line("for (", ColumnValue, " v : rows.get(row)) {");
                    {
                        line("columns.add(v.getColumnName());");
                    }
                    line("}");
                    line("for (", ColumnValue, " v : oldData.get(row)) {");
                    {
                        line("if (columns.contains(v.getColumnName())) {");
                        {
                            line("cellsAffected.put(row, v);");
                        }
                        line("}");
                    }
                    line("}");
                }
                line("}");
                line("return cellsAffected;");
            }
            line("}");
        }

        private void renderNamedPut() {
            line("@Override");
            line("public void put(Multimap<", Row, ", ? extends ", ColumnValue, "> rows) {");
            {
                line("t.useTable(tableRef, this);");

                if (!cellReferencingIndices.isEmpty()) {
                    line("Multimap<", Row, ", ", ColumnValue, "> affectedCells = getAffectedCells(rows);");
                    for (IndexMetadata index : cellReferencingIndices) {
                        String indexName = Renderers.getIndexTableName(index);
                        line("delete", indexName, "(affectedCells);");
                    }
                }

                if (!indices.isEmpty()) {
                    line("for (Entry<", Row, ", ? extends ", ColumnValue, "> e : rows.entries()) {");
                    {
                        for (IndexMetadata index : indices) {
                            renderIndexPut(index);
                        }
                    }
                    line("}");
                }
                line("t.put(tableRef, ColumnValues.toCellValues(rows));");
                line("for (", Trigger, " trigger : triggers) {");
                {
                    line("trigger.put", tableName, "(rows);");
                }
                line("}");
            }
            line("}");
        }

        private void renderCellReferencingIndexDelete(IndexMetadata index) {
            String indexName = Renderers.getIndexTableName(index);
            line("private void delete", indexName, "(Multimap<", Row, ", ", ColumnValue, "> result) {");
            {
                List<String> rowArgumentNames = new ArrayList<>();
                List<String> colArgumentNames = new ArrayList<>();
                List<TypeAndName> iterableArgNames = new ArrayList<>();

                boolean hasCol = index.getColumnNameToAccessData() != null;
                String columnClass = hasCol ? Renderers.CamelCase(index.getColumnNameToAccessData()) : null;

                line("ImmutableSet.Builder<Cell> indexCells = ImmutableSet.builder();");

                line("for (Entry<", Row, ", ", ColumnValue, "> e : result.entries()) {");
                {
                    if (columnClass != null) {
                        line("if (e.getValue() instanceof ", columnClass, ") ");
                    }
                    lineEnd("{");
                    {
                        if (columnClass != null) {
                            line(columnClass, " col = (", columnClass, ") e.getValue();");
                        }
                        if (index.getIndexCondition() != null) {
                            line("if (" + index.getIndexCondition().getValueCode("col.getValue()") + ") ");
                        }
                        lineEnd("{");
                        {
                            line(Row, " row = e.getKey();");

                            for (IndexComponent component : index.getRowComponents()) {
                                String varName = renderIndexComponent(component);
                                rowArgumentNames.add(varName);
                                if (component.isMultiple()) {
                                    iterableArgNames.add(new TypeAndName(
                                            component
                                                    .getRowKeyDescription()
                                                    .getType()
                                                    .getJavaClassName(),
                                            varName));
                                }
                            }

                            Collections.addAll(
                                    colArgumentNames, "row.persistToBytes()", "e.getValue().persistColumnName()");

                            for (IndexComponent component : index.getColumnComponents()) {
                                String varName = renderIndexComponent(component);
                                colArgumentNames.add(varName);
                                if (component.isMultiple()) {
                                    iterableArgNames.add(new TypeAndName(
                                            component
                                                    .getRowKeyDescription()
                                                    .getType()
                                                    .getJavaClassName(),
                                            varName));
                                }
                            }

                            for (TypeAndName iterableArg : iterableArgNames) {
                                line("for (", iterableArg.toString(), " : ", iterableArg.name, "Iterable) {");
                            }

                            line(
                                    indexName,
                                    "Table.",
                                    indexName,
                                    "Row indexRow = ",
                                    indexName,
                                    "Table.",
                                    indexName,
                                    "Row.of(",
                                    Joiner.on(", ").join(rowArgumentNames),
                                    ");");
                            line(
                                    indexName,
                                    "Table.",
                                    indexName,
                                    "Column indexCol = ",
                                    indexName,
                                    "Table.",
                                    indexName,
                                    "Column.of(",
                                    Joiner.on(", ").join(colArgumentNames),
                                    ");");
                            line("indexCells.add(Cell.create(indexRow.persistToBytes(), indexCol.persistToBytes()));");

                            for (int i = 0; i < iterableArgNames.size(); i++) {
                                line("}");
                            }
                        }
                        line("}");
                    }
                    line("}");
                }
                line("}");
                line(
                        "t.delete(",
                        Schemas.getTableReferenceString(index.getIndexName(), namespace),
                        ", indexCells.build());");
            }
            line("}");
        }

        private void renderNamedDeleteColumn(NamedColumnDescription col) {
            Collection<IndexMetadata> columnIndices = new ArrayList<>();
            for (IndexMetadata index : cellReferencingIndices) {
                if (col.getLongName().equals(index.getColumnNameToAccessData())) {
                    columnIndices.add(index);
                }
            }
            line("public void delete", ColumnRenderers.VarName(col), "(", Row, " row) {");
            {
                line("delete", ColumnRenderers.VarName(col), "(ImmutableSet.of(row));");
            }
            line("}");
            line();
            line("public void delete", ColumnRenderers.VarName(col), "(Iterable<", Row, "> rows) {");
            {
                line("byte[] col = PtBytes.toCachedBytes(", ColumnRenderers.short_name(col), ");");
                line("Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);");
                if (!columnIndices.isEmpty()) {
                    line("Map<Cell, byte[]> results = t.get(tableRef, cells);");
                    for (IndexMetadata index : columnIndices) {
                        line("delete", Renderers.getIndexTableName(index), "Raw(results);");
                    }
                }
                line("t.delete(tableRef, cells);");
            }
            line("}");
            for (IndexMetadata index : columnIndices) {
                line();
                renderNamedIndexDeleteRaw(col, index);
            }
        }

        private void renderNamedIndexDeleteRaw(NamedColumnDescription col, IndexMetadata index) {
            String indexName = Renderers.getIndexTableName(index);
            String NamedColumn = Renderers.CamelCase(col.getLongName());
            line("private void delete", indexName, "Raw(Map<Cell, byte[]> results) {");
            {
                List<String> rowArgumentNames = new ArrayList<>();
                List<String> colArgumentNames = new ArrayList<>();
                List<TypeAndName> iterableArgNames = new ArrayList<>();
                line("Set<Cell> indexCells = Sets.newHashSetWithExpectedSize(results.size());");
                line("for (Entry<Cell, byte[]> result : results.entrySet()) {");
                {
                    line(
                            NamedColumn,
                            " col = (",
                            NamedColumn,
                            ") shortNameToHydrator.get(",
                            ColumnRenderers.short_name(col),
                            ").hydrateFromBytes(result.getValue());");

                    line(Row, " row = ", Row, ".BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getRowName());");

                    for (IndexComponent component : index.getRowComponents()) {
                        String varName = renderIndexComponent(component);
                        rowArgumentNames.add(varName);
                        if (component.isMultiple()) {
                            iterableArgNames.add(new TypeAndName(
                                    component.getRowKeyDescription().getType().getJavaClassName(), varName));
                        }
                    }
                    Collections.addAll(colArgumentNames, "row.persistToBytes()", "col.persistColumnName()");

                    for (IndexComponent component : index.getColumnComponents()) {
                        String varName = renderIndexComponent(component);
                        colArgumentNames.add(varName);
                        if (component.isMultiple()) {
                            iterableArgNames.add(new TypeAndName(
                                    component.getRowKeyDescription().getType().getJavaClassName(), varName));
                        }
                    }

                    for (TypeAndName iterableArg : iterableArgNames) {
                        line("for (", iterableArg.toString(), " : ", iterableArg.name, "Iterable) {");
                    }

                    line(
                            indexName,
                            "Table.",
                            indexName,
                            "Row indexRow = ",
                            indexName,
                            "Table.",
                            indexName,
                            "Row.of(",
                            Joiner.on(", ").join(rowArgumentNames),
                            ");");
                    line(
                            indexName,
                            "Table.",
                            indexName,
                            "Column indexCol = ",
                            indexName,
                            "Table.",
                            indexName,
                            "Column.of(",
                            Joiner.on(", ").join(colArgumentNames),
                            ");");
                    line("indexCells.add(Cell.create(indexRow.persistToBytes(), indexCol.persistToBytes()));");

                    for (int i = 0; i < iterableArgNames.size(); i++) {
                        line("}");
                    }
                }
                line("}");
                line("t.delete(", Schemas.getTableReferenceString(index.getIndexName(), namespace), ", indexCells);");
            }
            line("}");
        }

        private void renderNamedDelete() {
            line("@Override");
            line("public void delete(", Row, " row) {");
            {
                line("delete(ImmutableSet.of(row));");
            }
            line("}");
            line();
            line("@Override");
            line("public void delete(Iterable<", Row, "> rows) {");
            {
                if (!cellReferencingIndices.isEmpty()) {
                    line("Multimap<", Row, ", ", ColumnValue, "> result = getRowsMultimap(rows);");
                    for (IndexMetadata index : cellReferencingIndices) {
                        line("delete", Renderers.getIndexTableName(index), "(result);");
                    }
                }

                SortedSet<NamedColumnDescription> namedColumns = ColumnRenderers.namedColumns(table);
                line("List<byte[]> rowBytes = Persistables.persistAll(rows);");
                line(
                        "Set<Cell> cells = Sets.newHashSetWithExpectedSize(rowBytes.size()",
                        ((namedColumns.size() == 1) ? "" : " * " + namedColumns.size()),
                        ");");
                for (NamedColumnDescription col : namedColumns) {
                    line(
                            "cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes(",
                            ColumnRenderers.short_name(col),
                            ")));");
                }
                line("t.delete(tableRef, cells);");
            }
            line("}");
        }

        private void renderOptimizeRangeRequests() {
            line("private RangeRequest optimizeRangeRequest(RangeRequest range) {");
            {
                line("if (range.getColumnNames().isEmpty()) {");
                {
                    line("return range.getBuilder().retainColumns(allColumns).build();");
                }
                line("}");
                line("return range;");
            }
            line("}");
            line();
            line("private Iterable<RangeRequest> optimizeRangeRequests(Iterable<RangeRequest> ranges) {");
            {
                line("return Iterables.transform(ranges, this::optimizeRangeRequest);");
            }
            line("}");
        }

        private void renderGetRange() {
            line("public BatchingVisitableView<", RowResult, "> getRange(RangeRequest range) {");
            {
                line(
                        "return BatchingVisitables.transform(t.getRange(tableRef, optimizeRangeRequest(range)), new"
                                + " Function<RowResult<byte[]>, ",
                        RowResult,
                        ">() {");
                {
                    line("@Override");
                    line("public ", RowResult, " apply(RowResult<byte[]> input) {");
                    {
                        line("return ", RowResult, ".of(input);");
                    }
                    line("}");
                }
                line("});");
            }
            line("}");
        }

        private void renderGetRanges() {
            line("@Deprecated");
            line("public IterableView<BatchingVisitable<", RowResult, ">> getRanges(Iterable<RangeRequest> ranges) {");
            {
                line("Iterable<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRanges(tableRef,"
                        + " optimizeRangeRequests(ranges));");
                line("return IterableView.of(rangeResults).transform(");
                line(
                        "        new Function<BatchingVisitable<RowResult<byte[]>>, BatchingVisitable<",
                        RowResult,
                        ">>() {");
                {
                    line("@Override");
                    line(
                            "public BatchingVisitable<",
                            RowResult,
                            "> apply(BatchingVisitable<RowResult<byte[]>> visitable) {");
                    {
                        line(
                                "return BatchingVisitables.transform(visitable, new Function<RowResult<byte[]>, ",
                                RowResult,
                                ">() {");
                        {
                            line("@Override");
                            line("public ", RowResult, " apply(RowResult<byte[]> row) {");
                            {
                                line("return ", RowResult, ".of(row);");
                            }
                            line("}");
                        }
                        line("});");
                    }
                    line("}");
                }
                line("});");
            }
            line("}");
            line();
            line("public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,");
            line("                               int concurrencyLevel,");
            line(
                    "                               BiFunction<RangeRequest, BatchingVisitable<",
                    RowResult,
                    ">, T> visitableProcessor) {");
            {
                line("return t.getRanges(ImmutableGetRangesQuery.<T>builder()");
                line("                    .tableRef(tableRef)");
                line("                    .rangeRequests(ranges)");
                line("                    .rangeRequestOptimizer(this::optimizeRangeRequest)");
                line("                    .concurrencyLevel(concurrencyLevel)");
                line("                    .visitableProcessor((rangeRequest, visitable) ->");
                line("                            visitableProcessor.apply(rangeRequest,");
                line(
                        "                                    BatchingVisitables.transform(visitable, ",
                        RowResult,
                        "::of)))");
                line("                    .build());");
            }
            line("}");
            line();
            line("public <T> Stream<T> getRanges(Iterable<RangeRequest> ranges,");
            line(
                    "                               BiFunction<RangeRequest, BatchingVisitable<",
                    RowResult,
                    ">, T> visitableProcessor) {");
            {
                line("return t.getRanges(ImmutableGetRangesQuery.<T>builder()");
                line("                    .tableRef(tableRef)");
                line("                    .rangeRequests(ranges)");
                line("                    .rangeRequestOptimizer(this::optimizeRangeRequest)");
                line("                    .visitableProcessor((rangeRequest, visitable) ->");
                line("                            visitableProcessor.apply(rangeRequest,");
                line(
                        "                                    BatchingVisitables.transform(visitable, ",
                        RowResult,
                        "::of)))");
                line("                    .build());");
            }
            line("}");
            line();
            line("public Stream<BatchingVisitable<", RowResult, ">> getRangesLazy(Iterable<RangeRequest> ranges) {");
            {
                line("Stream<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRangesLazy(tableRef,"
                        + " optimizeRangeRequests(ranges));");
                line(
                        "return rangeResults.map(visitable -> BatchingVisitables.transform(visitable, ",
                        RowResult,
                        "::of));");
            }
            line("}");
        }

        private void renderDeleteRange() {
            line("public void deleteRange(RangeRequest range) {");
            {
                line("deleteRanges(ImmutableSet.of(range));");
            }
            line("}");
        }

        private void renderDynamicDeleteRanges() {
            line("public void deleteRanges(Iterable<RangeRequest> ranges) {");
            {
                line(
                        "BatchingVisitables.concat(getRanges(ranges)).batchAccept(1000, new AbortingVisitor<List<",
                        RowResult,
                        ">, RuntimeException>() {");
                {
                    line("@Override");
                    line("public boolean visit(List<", RowResult, "> rowResults) {");
                    {
                        line("Multimap<", Row, ", ", Column, "> toRemove = HashMultimap.create();");
                        line("for (", RowResult, " rowResult : rowResults) {");
                        {
                            line("for (", ColumnValue, " columnValue : rowResult.getColumnValues()) {");
                            {
                                line("toRemove.put(rowResult.getRowName(), columnValue.getColumnName());");
                            }
                            line("}");
                        }
                        line("}");
                        line("delete(toRemove);");
                        line("return true;");
                    }
                    line("}");
                }
                line("});");
            }
            line("}");
        }

        private void renderNamedDeleteRanges() {
            line("public void deleteRanges(Iterable<RangeRequest> ranges) {");
            {
                line("BatchingVisitables.concat(getRanges(ranges))");
                line("                  .transform(", RowResult, ".getRowNameFun())");
                line("                  .batchAccept(1000, new AbortingVisitor<List<", Row, ">, RuntimeException>() {");
                {
                    line("@Override");
                    line("public boolean visit(List<", Row, "> rows) {");
                    {
                        line("delete(rows);");
                        line("return true;");
                    }
                    line("}");
                }
                line("});");
            }
            line("}");
        }

        private void renderOptimizeColumnSelection() {
            line("private ColumnSelection optimizeColumnSelection(ColumnSelection columns) {");
            {
                line("if (columns.allColumnsSelected()) {");
                {
                    line("return allColumns;");
                }
                line("}");
                line("return columns;");
            }
            line("}");
        }

        private void renderGetAllRowsUnordered() {
            line("public BatchingVisitableView<", RowResult, "> getAllRowsUnordered() {");
            {
                line("return getAllRowsUnordered(allColumns);");
            }
            line("}");
            line();
            line("public BatchingVisitableView<", RowResult, "> getAllRowsUnordered(ColumnSelection columns) {");
            {
                line("return BatchingVisitables.transform(t.getRange(tableRef, RangeRequest.builder()");
                line("        .retainColumns(optimizeColumnSelection(columns)).build()),");
                line("        new Function<RowResult<byte[]>, ", RowResult, ">() {");
                {
                    line("@Override");
                    line("public ", RowResult, " apply(RowResult<byte[]> input) {");
                    {
                        line("return ", RowResult, ".of(input);");
                    }
                    line("}");
                }
                line("});");
            }
            line("}");
        }

        private void renderNamedGetRow() {
            line("public ", "Optional<", RowResult, ">", " getRow(", Row, " row) {");
            {
                line("return getRow(row, allColumns);");
            }
            line("}");
            line();
            line("public ", "Optional<", RowResult, ">", " getRow(", Row, " row, ColumnSelection columns) {");
            {
                line("byte[] bytes = row.persistToBytes();");
                line("RowResult<byte[]> rowResult = t.getRows(tableRef, ImmutableSet.of(bytes), columns).get(bytes);");
                line("if (rowResult == null) {");
                {
                    renderOptionalReturnValue(null);
                }
                line("} else {");
                {
                    renderOptionalReturnValue("rowResult");
                }
                line("}");
            }
            line("}");
        }

        private void renderOptionalReturnValue(@Nullable String value) {
            if (value != null) {
                line("return Optional.of(", RowResult, ".of(rowResult));");
            } else {
                line("return Optional.", optionalType.nullMethod(), "();");
            }
        }

        private void renderNamedGetRows() {
            line("@Override");
            line("public List<", RowResult, "> getRows(Iterable<", Row, "> rows) {");
            {
                line("return getRows(rows, allColumns);");
            }
            line("}");
            line();
            line("@Override");
            line("public List<", RowResult, "> getRows(Iterable<", Row, "> rows, ColumnSelection columns) {");
            {
                line("SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef,"
                        + " Persistables.persistAll(rows), columns);");
                line("List<", RowResult, "> rowResults = Lists.newArrayListWithCapacity(results.size());");
                line("for (RowResult<byte[]> row : results.values()) {");
                {
                    line("rowResults.add(", RowResult, ".of(row));");
                }
                line("}");
                line("return rowResults;");
            }
            line("}");
        }

        private void renderDynamicGet() {
            line("@Override");
            line("public Multimap<", Row, ", ", ColumnValue, "> get(Multimap<", Row, ", ", Column, "> cells) {");
            {
                line("Set<Cell> rawCells = ColumnValues.toCells(cells);");
                line("Map<Cell, byte[]> rawResults = t.get(tableRef, rawCells);");
                line("Multimap<", Row, ", ", ColumnValue, "> rowMap = HashMultimap.create();");
                line("for (Entry<Cell, byte[]> e : rawResults.entrySet()) {");
                {
                    line("if (e.getValue().length > 0) {");
                    {
                        line(Row, " row = ", Row, ".BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());");
                        line(
                                Column,
                                " col = ",
                                Column,
                                ".BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());");
                        line(
                                table.getColumns().getDynamicColumn().getValue().getJavaObjectTypeName(),
                                " val = ",
                                ColumnValue,
                                ".hydrateValue(e.getValue());");
                        line("rowMap.put(row, ", ColumnValue, ".of(col, val));");
                    }
                    line("}");
                }
                line("}");
                line("return rowMap;");
            }
            line("}");
        }

        private void renderGetRowsMultimap(boolean isDynamic) {
            line("@Override");
            line("public Multimap<", Row, ", ", ColumnValue, "> getRowsMultimap(Iterable<", Row, "> rows) {");
            {
                line("return getRowsMultimapInternal(rows, allColumns);");
            }
            line("}");
            line();
            line("@Override");
            line(
                    "public Multimap<",
                    Row,
                    ", ",
                    ColumnValue,
                    "> getRowsMultimap(Iterable<",
                    Row,
                    "> rows, ColumnSelection columns) {");
            {
                line("return getRowsMultimapInternal(rows, columns);");
            }
            line("}");
            line();
            line(
                    "private Multimap<",
                    Row,
                    ", ",
                    ColumnValue,
                    "> getRowsMultimapInternal(Iterable<",
                    Row,
                    "> rows, ColumnSelection columns) {");
            {
                line("SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef,"
                        + " Persistables.persistAll(rows), columns);");
                line("return getRowMapFromRowResults(results.values());");
            }
            line("}");
            line();
            line(
                    "private static Multimap<",
                    Row,
                    ", ",
                    ColumnValue,
                    "> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {");
            {
                line("Multimap<", Row, ", ", ColumnValue, "> rowMap = HashMultimap.create();");
                line("for (RowResult<byte[]> result : rowResults) {");
                {
                    line(Row, " row = ", Row, ".BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());");
                    line("for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {");
                    {
                        if (isDynamic) {
                            line(Column, " col = ", Column, ".BYTES_HYDRATOR.hydrateFromBytes(e.getKey());");
                            line(
                                    table.getColumns()
                                            .getDynamicColumn()
                                            .getValue()
                                            .getJavaObjectTypeName(),
                                    " val = ",
                                    ColumnValue,
                                    ".hydrateValue(e.getValue());");
                            line("rowMap.put(row, ", ColumnValue, ".of(col, val));");
                        } else {
                            line(
                                    "rowMap.put(row,"
                                        + " shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));");
                        }
                    }
                    line("}");
                }
                line("}");
                line("return rowMap;");
            }
            line("}");
        }

        private void renderGetRowsColumnRange(boolean isDynamic) {
            line("@Override");
            line(
                    "public Map<",
                    Row,
                    ", BatchingVisitable<",
                    ColumnValue,
                    ">> getRowsColumnRange(Iterable<",
                    Row,
                    "> rows, BatchColumnRangeSelection columnRangeSelection) {");
            {
                line("Map<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> results ="
                        + " t.getRowsColumnRange(tableRef, Persistables.persistAll(rows), columnRangeSelection);");
                line(
                        "Map<",
                        Row,
                        ", BatchingVisitable<",
                        ColumnValue,
                        ">> transformed = Maps.newHashMapWithExpectedSize(results.size());");
                line("for (Entry<byte[], BatchingVisitable<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {");
                {
                    line(Row, " row = ", Row, ".BYTES_HYDRATOR.hydrateFromBytes(e.getKey());");
                    line(
                            "BatchingVisitable<",
                            ColumnValue,
                            "> bv = BatchingVisitables.transform(e.getValue(), result -> {");
                    {
                        if (isDynamic) {
                            line(
                                    Column,
                                    " col = ",
                                    Column,
                                    ".BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());");
                            line(
                                    table.getColumns()
                                            .getDynamicColumn()
                                            .getValue()
                                            .getJavaObjectTypeName(),
                                    " val = ",
                                    ColumnValue,
                                    ".hydrateValue(result.getValue());");
                            line("return ", ColumnValue, ".of(col, val);");
                        } else {
                            line(
                                    "return"
                                        + " shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());");
                        }
                    }
                    line("});");
                    line("transformed.put(row, bv);");
                }
                line("}");
                line("return transformed;");
            }
            line("}");
            line();
            line("@Override");
            line(
                    "public Iterator<Map.Entry<",
                    Row,
                    ", ",
                    ColumnValue,
                    ">> getRowsColumnRange(Iterable<",
                    Row,
                    "> rows, ColumnRangeSelection columnRangeSelection, int batchHint) {");
            {
                line("Iterator<Map.Entry<Cell, byte[]>> results = t.getRowsColumnRange(getTableRef(),"
                        + " Persistables.persistAll(rows), columnRangeSelection, batchHint);");
                line("return Iterators.transform(results, e -> {");
                {
                    line(Row, " row = ", Row, ".BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());");
                    if (isDynamic) {
                        line(
                                Column,
                                " col = ",
                                Column,
                                ".BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());");
                        line(
                                table.getColumns().getDynamicColumn().getValue().getJavaObjectTypeName(),
                                " val = ",
                                ColumnValue,
                                ".hydrateValue(e.getValue());");
                        line(ColumnValue, " colValue = ", ColumnValue, ".of(col, val);");
                    } else {
                        line(
                                ColumnValue,
                                " colValue ="
                                    + " shortNameToHydrator.get(PtBytes.toString(e.getKey().getColumnName())).hydrateFromBytes(e.getValue());");
                    }
                    line("return Maps.immutableEntry(row, colValue);");
                }
                line("});");
            }
            line("}");
        }

        private void renderGetRowsColumnRangeIterator(boolean isDynamic) {
            line("@Override");
            line(
                    "public Map<",
                    Row,
                    ", Iterator<",
                    ColumnValue,
                    ">> getRowsColumnRangeIterator(Iterable<",
                    Row,
                    "> rows, BatchColumnRangeSelection columnRangeSelection) {");
            {
                line("Map<byte[], Iterator<Map.Entry<Cell, byte[]>>> results ="
                        + " t.getRowsColumnRangeIterator(tableRef, Persistables.persistAll(rows),"
                        + " columnRangeSelection);");
                line(
                        "Map<",
                        Row,
                        ", Iterator<",
                        ColumnValue,
                        ">> transformed = Maps.newHashMapWithExpectedSize(results.size());");
                line("for (Entry<byte[], Iterator<Map.Entry<Cell, byte[]>>> e : results.entrySet()) {");
                {
                    line(Row, " row = ", Row, ".BYTES_HYDRATOR.hydrateFromBytes(e.getKey());");
                    line("Iterator<", ColumnValue, "> bv = Iterators.transform(e.getValue(), result -> {");
                    {
                        if (isDynamic) {
                            line(
                                    Column,
                                    " col = ",
                                    Column,
                                    ".BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getColumnName());");
                            line(
                                    table.getColumns()
                                            .getDynamicColumn()
                                            .getValue()
                                            .getJavaObjectTypeName(),
                                    " val = ",
                                    ColumnValue,
                                    ".hydrateValue(result.getValue());");
                            line("return ", ColumnValue, ".of(col, val);");
                        } else {
                            line(
                                    "return"
                                        + " shortNameToHydrator.get(PtBytes.toString(result.getKey().getColumnName())).hydrateFromBytes(result.getValue());");
                        }
                    }
                    line("});");
                    line("transformed.put(row, bv);");
                }
                line("}");
                line("return transformed;");
            }
            line("}");
        }

        private void renderFindConstraintFailures() {
            line("@Override");
            line("public List<String> findConstraintFailures(Map<Cell, byte[]> writes,");
            line("                                           ConstraintCheckingTransaction transaction,");
            line("                                           AtlasDbConstraintCheckingMode constraintCheckingMode) {");
            {
                line("return ImmutableList.of();");
            }
            line("}");
            line();
            line("@Override");
            line("public List<String> findConstraintFailuresNoRead(Map<Cell, byte[]> writes,");
            line("                                                 AtlasDbConstraintCheckingMode"
                    + " constraintCheckingMode) {");
            {
                line("return ImmutableList.of();");
            }
            line("}");
        }

        private void renderAdd() {
            line("@Override");
            line("public void add(", Row, " row) {");
            {
                line("add(ImmutableSet.of(row));");
            }
            line("}");
            line();
            line("@Override");
            line("public void add(Set<", Row, "> rows) {");
            {
                line("Map<", Row, ", ", ColumnValue, "> map = Maps.newHashMapWithExpectedSize(rows.size());");
                line(ColumnValue, " col = Exists.of(0L);");
                line("for (", Row, " row : rows) {");
                {
                    line("map.put(row, col);");
                }
                line("}");
                line("put(Multimaps.forMap(map));");
            }
            line("}");
            line();
            line("@Override");
            line("public void addUnlessExists(", Row, " row) {");
            {
                line("addUnlessExists(ImmutableSet.of(row));");
            }
            line("}");
            line();
            line("@Override");
            line("public void addUnlessExists(Set<", Row, "> rows) {");
            {
                line("SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableRef,"
                        + " Persistables.persistAll(rows), allColumns);");
                line(
                        "Map<",
                        Row,
                        ", ",
                        ColumnValue,
                        "> map = Maps.newHashMapWithExpectedSize(rows.size() - results.size());");
                line(ColumnValue, " col = Exists.of(0L);");
                line("for (", Row, " row : rows) {");
                {
                    line("if (!results.containsKey(row.persistToBytes())) {");
                    {
                        line("map.put(row, col);");
                    }
                    line("}");
                }
                line("}");
                line("put(Multimaps.forMap(map));");
            }
            line("}");
        }

        private void renderClassHash() {
            byte[] hash = getHash();
            line("static String __CLASS_HASH = \"", BaseEncoding.base64().encode(hash), "\";");
        }
    }

    // ================================================== //
    // Nothing after this point should have side-effects. //
    // ================================================== //

    private static boolean isNamedSet(TableMetadata table) {
        Set<NamedColumnDescription> namedColumns = table.getColumns().getNamedColumns();
        return namedColumns != null
                && namedColumns.size() == 1
                && Iterables.getOnlyElement(namedColumns).getLongName().equals("exists");
    }

    private static boolean isDynamic(TableMetadata table) {
        return table.getColumns().hasDynamicColumns();
    }

    private static Collection<IndexMetadata> getCellReferencingIndices(SortedSet<IndexMetadata> indices) {
        return Collections2.filter(indices, index -> index.getIndexType() == IndexType.CELL_REFERENCING);
    }

    private static List<Class<?>> getImports(OptionalType optionalType) {
        List<Class<?>> classes = new ArrayList<>();
        classes.addAll(Arrays.asList(IMPORTS_WITHOUT_OPTIONAL));
        switch (optionalType) {
            case GUAVA:
                classes.add(com.google.common.base.Optional.class);
                break;
            case JAVA8:
                classes.add(Optional.class);
                break;
            default:
                throw new SafeIllegalArgumentException("Unknown optionalType!");
        }
        return classes;
    }

    private static final Class<?>[] IMPORTS_WITHOUT_OPTIONAL = {
        Set.class,
        List.class,
        Map.class,
        SortedMap.class,
        Callable.class,
        Multimap.class,
        Multimaps.class,
        Collection.class,
        Function.class,
        BiFunction.class,
        Persistable.class,
        Hydrator.class,
        Transaction.class,
        NamedColumnValue.class,
        ColumnValue.class,
        BatchingVisitable.class,
        RangeRequest.class,
        Prefix.class,
        BatchingVisitables.class,
        BatchingVisitableView.class,
        IterableView.class,
        ColumnValues.class,
        RowResult.class,
        Persistables.class,
        Maps.class,
        Lists.class,
        ImmutableMap.class,
        ImmutableSet.class,
        Sets.class,
        HashSet.class,
        HashMultimap.class,
        ArrayListMultimap.class,
        ImmutableMultimap.class,
        Cell.class,
        Cells.class,
        EncodingUtils.class,
        PtBytes.class,
        MoreObjects.class,
        Objects.class,
        ComparisonChain.class,
        Sha256Hash.class,
        UUID.class,
        EnumSet.class,
        Descending.class,
        AbortingVisitor.class,
        AbortingVisitors.class,
        AssertUtils.class,
        AtlasDbConstraintCheckingMode.class,
        ConstraintCheckingTransaction.class,
        AtlasDbDynamicMutablePersistentTable.class,
        AtlasDbNamedPersistentSet.class,
        AtlasDbMutablePersistentTable.class,
        AtlasDbNamedMutableTable.class,
        ColumnSelection.class,
        Joiner.class,
        Map.Entry.class,
        Iterator.class,
        Iterables.class,
        Stream.class,
        Supplier.class,
        InvalidProtocolBufferException.class,
        Throwables.class,
        ImmutableList.class,
        UnsignedBytes.class,
        Collections2.class,
        Arrays.class,
        Bytes.class,
        TypedRowResult.class,
        TimeUnit.class,
        CompressionUtils.class,
        Compression.class,
        Namespace.class,
        Hashing.class,
        ValueType.class,
        Generated.class,
        TableReference.class,
        BatchColumnRangeSelection.class,
        ColumnRangeSelections.class,
        ColumnRangeSelection.class,
        Iterators.class,
        ImmutableGetRangesQuery.class,
    };
}
