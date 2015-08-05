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
package com.palantir.atlasdb.table.description.render;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.UnsignedBytes;
import com.google.protobuf.InvalidProtocolBufferException;
import com.palantir.atlasdb.compress.CompressionUtils;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.Prefix;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.impl.Cells;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ExpirationStrategy;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.schema.Namespace;
import com.palantir.atlasdb.table.api.AtlasDbDynamicMutableExpiringTable;
import com.palantir.atlasdb.table.api.AtlasDbDynamicMutablePersistentTable;
import com.palantir.atlasdb.table.api.AtlasDbMutableExpiringTable;
import com.palantir.atlasdb.table.api.AtlasDbMutablePersistentTable;
import com.palantir.atlasdb.table.api.AtlasDbNamedExpiringSet;
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
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.generation.ColumnValues;
import com.palantir.atlasdb.table.generation.Descending;
import com.palantir.atlasdb.table.generation.NamedColumnValue;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.ConstraintCheckingTransaction;
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
import com.palantir.common.proxy.AsyncProxy;
import com.palantir.util.AssertUtils;
import com.palantir.util.crypto.Sha256Hash;

public class TableRenderer {
    private final String packageName;
    private final Namespace namespace;

    public TableRenderer(String packageName, Namespace namespace) {
        this.packageName = Preconditions.checkNotNull(packageName);
        this.namespace = Preconditions.checkNotNull(namespace);
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
        private final String Table;
        private final String Row;
        private final String Column;
        private final String ColumnValue;
        private final String RowResult;
        private final String Trigger;

        public ClassRenderer(String rawTableName,
                             TableDefinition table,
                             SortedSet<IndexMetadata> indices) {
            this.tableName = Renderers.getClassTableName(rawTableName, table);
            this.table = table.toTableMetadata();
            this.indices = indices;
            this.cellReferencingIndices = getCellReferencingIndices(indices);
            this.raw_table_name = Schemas.getFullTableName(rawTableName, namespace);
            this.isGeneric = table.getGenericTableName() != null;
            this.isNestedIndex = false;
            this.Table = tableName + "Table";
            this.Row = tableName + "Row";
            this.Column = tableName + (isDynamic(this.table) ? "Column" : "NamedColumn");
            this.ColumnValue = tableName + (isDynamic(this.table) ? "ColumnValue" : "NamedColumnValue<?>");
            this.RowResult = tableName + "RowResult";
            this.Trigger = tableName + "Trigger";
        }

        public ClassRenderer(Renderer parent, IndexMetadata index) {
            super(parent);
            this.tableName = Renderers.getIndexTableName(index);
            this.table = index.getTableMetadata();
            this.indices = ImmutableSortedSet.of();
            this.cellReferencingIndices = ImmutableList.of();
            this.raw_table_name = Schemas.getFullTableName(index.getIndexName(), namespace);
            this.isGeneric = false;
            this.isNestedIndex = true;
            this.Table = tableName + "Table";
            this.Row = tableName + "Row";
            this.Column = tableName + (isDynamic(this.table) ? "Column" : "NamedColumn");
            this.ColumnValue = tableName + (isDynamic(this.table) ? "ColumnValue" : "NamedColumnValue<?>");
            this.RowResult = tableName + "RowResult";
            this.Trigger = tableName + "Trigger";
        }

        @Override
        protected void run() {
            ImportRenderer importRenderer = new ImportRenderer(this,
                    Arrays.asList(IMPORTS));
            if (!isNestedIndex) {
                _("package ", packageName, ";");
                _();
                importRenderer.renderImports();
                _();
            }
            _("public ", isNestedIndex ? "static " : "", "final class ", Table, " implements");
            if (isNamedSet(table)) {
                if (isExpiring(table)) {
                    _("        AtlasDbNamedExpiringSet<", Table, ".", Row, ">,");
                } else {
                    _("        AtlasDbNamedPersistentSet<", Table, ".", Row, ">,");
                }
            }
            if (isDynamic(table)) {
                if (isExpiring(table)) {
                    _("        AtlasDbDynamicMutableExpiringTable<", Table, ".", Row, ",");
                    _("                                              ", Table, ".", Column, ",");
                    _("                                              ", Table, ".", ColumnValue, ",");
                    _("                                              ", Table, ".", RowResult, "> {");
                } else {
                    _("        AtlasDbDynamicMutablePersistentTable<", Table, ".", Row, ",");
                    _("                                                ", Table, ".", Column, ",");
                    _("                                                ", Table, ".", ColumnValue, ",");
                    _("                                                ", Table, ".", RowResult, "> {");
                }
            } else {
                if (isExpiring(table)) {
                    _("        AtlasDbMutableExpiringTable<", Table, ".", Row, ",");
                    _("                                       ", Table, ".", ColumnValue, ",");
                    _("                                       ", Table, ".", RowResult, ">,");
                } else {
                    _("        AtlasDbMutablePersistentTable<", Table, ".", Row, ",");
                    _("                                         ", Table, ".", ColumnValue, ",");
                    _("                                         ", Table, ".", RowResult, ">,");
                }
                _("        AtlasDbNamedMutableTable<", Table, ".", Row, ",");
                _("                                    ", Table, ".", ColumnValue, ",");
                _("                                    ", Table, ".", RowResult, "> {");
            } {
                fields();
                _();
                staticFactories();
                _();
                constructors();
                _();
                renderGetTableName();
                _();
                new RowOrDynamicColumnRenderer(this, Row, table.getRowMetadata(), table.isRangeScanAllowed()).run();
                _();
                if (isDynamic(table)) {
                    renderDynamic();
                } else {
                    renderNamed();
                }
                _();
                if (table.isRangeScanAllowed()) {
                    renderGetRange();
                    _();
                    renderGetRanges();
                    _();
                    renderDeleteRange();
                    _();
                    if (isDynamic(table)) {
                        renderDynamicDeleteRanges();
                    } else {
                        renderNamedDeleteRanges();
                    }
                } else {
                    renderGetAllRowsUnordered();
                }
                _();
                if (isNamedSet(table)) {
                    renderAdd();
                    _();
                }
                renderFindConstraintFailures();
                for (IndexMetadata index : indices) {
                    _();
                    new ClassRenderer(this, index).run();
                }
                if (!isNestedIndex) {
                    _();
                    importRenderer.renderImportJavaDoc();
                    renderClassHash();
                }
            } _("}");
            _();
        }

        private void renderNamed() {
            _("public interface ", tableName, "NamedColumnValue<T> extends NamedColumnValue<T> { /* */ }");
            _();
            for (NamedColumnDescription col : ColumnRenderers.namedColumns(table)) {
                new NamedColumnValueRenderer(this, tableName, col).run();
                _();
            }
            renderTrigger();
            _();
            new NamedRowResultRenderer(this, tableName, ColumnRenderers.namedColumns(table)).run();
            _();
            new NamedColumnRenderer(this, tableName, ColumnRenderers.namedColumns(table)).run();
            _();
            renderColumnSelection(false);
            _();
            renderShortNameToHydrator();
            _();
            for (NamedColumnDescription col : table.getColumns().getNamedColumns()) {
                renderNamedGetColumn(col);
                _();
            }
            for (NamedColumnDescription col : table.getColumns().getNamedColumns()) {
                renderNamedPutColumn(col);
                _();
            }
            renderNamedPut();
            _();
            for (NamedColumnDescription col : table.getColumns().getNamedColumns()) {
                renderNamedDeleteColumn(col);
                _();
            }
            renderNamedDelete();
            _();
            renderNamedGetRow();
            _();
            renderNamedGetRows();
            _();
            renderGetRowColumns(false);
            _();
            renderGetRowsMultimap(false);

            if (!cellReferencingIndices.isEmpty()) {
                _();
                renderNamedGetAffectedCells();
                for (IndexMetadata index : cellReferencingIndices) {
                    _();
                    renderCellReferencingIndexDelete(index);
                }
            }
        }

        private void renderDynamic() {
            new RowOrDynamicColumnRenderer(this, Column, table.getColumns().getDynamicColumn().getColumnNameDesc(), false).run();
            _();
            renderTrigger();
            _();
            new DynamicColumnValueRenderer(this, tableName, table.getColumns().getDynamicColumn()).run();
            _();
            new DynamicRowResultRenderer(this, tableName, table.getColumns().getDynamicColumn().getValue()).run();
            _();
            renderDynamicDelete();
            _();
            renderDynamicPut();
            _();
            if (!isExpiring(table)) {
                renderDynamicTouch();
                _();
            }
            renderColumnSelection(true);
            _();
            renderDynamicGet();
            _();
            renderGetRowColumns(true);
            _();
            renderGetRowsMultimap(true);
        }

        private void fields() {
            _("private final Transaction t;");
            _("private final List<", Trigger, "> triggers;");
            _("private final ", isGeneric ? "" : "static ", "String tableName", isGeneric ? ";" : " = \"" + raw_table_name + "\";");
        }

        private void staticFactories() {
            _(isNestedIndex ? "public " : "", "static ", Table, " of(Transaction t", isGeneric ? ", String tableName" : "", ") {"); {
                _("return new ", Table, "(t", isGeneric ? ", tableName" : "", ", ImmutableList.<", Trigger, ">of());");
            } _("}");
            _();
            _(isNestedIndex ? "public " : "", "static ", Table, " of(Transaction t", isGeneric ? ", String tableName" : "", ", ", Trigger, " trigger, ", Trigger, "... triggers) {"); {
                _("return new ", Table, "(t", isGeneric ? ", tableName" : "", ", ImmutableList.<", Trigger, ">builder().add(trigger).add(triggers).build());");
            } _("}");
            _();
            _(isNestedIndex ? "public " : "", "static ", Table, " of(Transaction t", isGeneric ? ", String tableName" : "", ", List<", Trigger, "> triggers) {"); {
                _("return new ", Table, "(t", isGeneric ? ", tableName" : "", ", triggers);");
            } _("}");
        }

        private void constructors() {
            _("private ", Table, "(Transaction t", isGeneric ? ", String tableName" : "", ", List<", Trigger, "> triggers) {"); {
                _("this.t = t;");
                if (isGeneric) {
                    _("this.tableName = tableName;");
                }
                _("this.triggers = triggers;");
            } _("}");
        }

        private void renderGetTableName() {
            _("public ", isGeneric ? "" : "static ", "String getTableName() {"); {
                _("return tableName;");
            } _("}");
        }

        private void renderTrigger() {
            _("public interface ", Trigger, " {"); {
                _("public void put", tableName, "(Multimap<", Row, ", ? extends ", ColumnValue, "> newRows);");
            } _("}");
        }

        private void renderDynamicDelete() {
            _("@Override");
            _("public void delete(", Row, " row, ", Column, " column) {"); {
                _("delete(ImmutableMultimap.of(row, column));");
            } _("}");
            _();
            _("@Override");
            _("public void delete(Iterable<", Row, "> rows) {"); {
                _("Multimap<", Row, ", ", Column, "> toRemove = HashMultimap.create();");
                _("Multimap<", Row, ", ", ColumnValue, "> result = getRowsMultimap(rows);");
                _("for (Entry<", Row, ", ", ColumnValue, "> e : result.entries()) {"); {
                    _("toRemove.put(e.getKey(), e.getValue().getColumnName());");
                } _("}");
                _("delete(toRemove);");
            } _("}");
            _();
            _("@Override");
            _("public void delete(Multimap<", Row, ", ", Column, "> values) {"); {
                _("t.delete(tableName, ColumnValues.toCells(values));");
            } _("}");
        }

        private void renderDynamicPut() {
            String firstParams = isExpiring(table) ? "long duration, TimeUnit unit, " : "";
            String lastParams = isExpiring(table) ? ", long duration, TimeUnit unit" : "";
            String args = isExpiring(table) ? ", duration, unit" : "";
            _("@Override");
            _("public void put(", Row, " rowName, Iterable<", ColumnValue, "> values", lastParams, ") {"); {
                _("put(ImmutableMultimap.<", Row, ", ", ColumnValue, ">builder().putAll(rowName, values).build()", args, ");");
            } _("}");
            _();
            _("@Override");
            _("public void put(", firstParams, Row, " rowName, ", ColumnValue, "... values) {"); {
                _("put(ImmutableMultimap.<", Row, ", ", ColumnValue, ">builder().putAll(rowName, values).build()", args, ");");
            } _("}");
            _();
            _("@Override");
            _("public void put(Multimap<", Row, ", ? extends ", ColumnValue, "> values", lastParams, ") {"); {
                _("t.useTable(tableName, this);");
                if (!indices.isEmpty()) {
                    _("for (Entry<", Row, ", ? extends ", ColumnValue, "> e : values.entries()) {"); {
                        for (IndexMetadata index : indices) {
                            renderIndexPut(index);
                        }
                    } _("}");
                }
                _("t.put(tableName, ColumnValues.toCellValues(values", args, "));");
                _("for (", Trigger, " trigger : triggers) {"); {
                    _("trigger.put", tableName, "(values);");
                } _("}");
            } _("}");
        }

        private void renderIndexPut(IndexMetadata index) {
            String args = isExpiring(table) ? ", duration, unit" : "";
            List<String> rowArgumentNames = Lists.newArrayList();
            List<String> colArgumentNames = Lists.newArrayList();
            List<TypeAndName> iterableArgNames = Lists.newArrayList();
            boolean hasCol = index.getColumnNameToAccessData() != null;
            String indexName = Renderers.getIndexTableName(index);
            String columnClass = null;
            if (hasCol) {
                columnClass = Renderers.CamelCase(index.getColumnNameToAccessData());
                _("if (e.getValue() instanceof ", columnClass, ")");
            } else if (isDynamic(table)) {
                columnClass = tableName + "ColumnValue";
            }
            _("{"); {
                if (hasCol || isDynamic(table)) {
                    _(columnClass, " col = (", columnClass, ") e.getValue();");
                }
                if (index.getIndexCondition() != null) {
                    _("if (" + index.getIndexCondition().getValueCode("col.getValue()") + ")");
                }
                _("{"); {
                    _(Row, " row = e.getKey();");
                    _(indexName, "Table table = ", indexName, "Table.of(t);");
                    for (IndexComponent component : index.getRowComponents()) {
                        String varName = renderIndexComponent(component);
                        rowArgumentNames.add(varName);
                        if (component.isMultiple()) {
                            iterableArgNames.add(new TypeAndName(component.getRowKeyDescription().getType().getJavaClassName(), varName));
                        }
                    }

                    if (index.getIndexType().equals(IndexType.CELL_REFERENCING)) {
                        Collections.addAll(colArgumentNames, "row.persistToBytes()", "e.getValue().persistColumnName()");
                    }

                    for (IndexComponent component : index.getColumnComponents()) {
                        String varName = renderIndexComponent(component);
                        colArgumentNames.add(varName);
                        if (component.isMultiple()) {
                            iterableArgNames.add(new TypeAndName(component.getRowKeyDescription().getType().getJavaClassName(), varName));
                        }
                    }

                    for (TypeAndName iterableArg : iterableArgNames) {
                        _("for (", iterableArg.toString(), " : ", iterableArg.name, "Iterable) {");
                    }

                    _(indexName, "Table.", indexName, "Row indexRow = ", indexName, "Table.", indexName, "Row.of(", Joiner.on(", ").join(rowArgumentNames), ");");
                    if (!index.isDynamicIndex() && !index.getIndexType().equals(IndexType.CELL_REFERENCING)) {
                        _("table.putExists(indexRow, 0L", args, ");");
                    } else {
                        _(indexName, "Table.", indexName, "Column indexCol = ", indexName, "Table.", indexName, "Column.of(", Joiner.on(", ").join(colArgumentNames), ");");
                        _(indexName, "Table.", indexName, "ColumnValue indexColVal = ", indexName, "Table.", indexName, "ColumnValue.of(indexCol, 0L);");
                        _("table.put(indexRow, indexColVal", args, ");");
                    }

                    for (int i = 0; i < iterableArgNames.size(); i++) {
                        _("}");
                    }
                } _("}");
            } _("}");
        }

        private String renderIndexComponent(IndexComponent component) {
            NameComponentDescription compDesc = component.getRowKeyDescription();
            String columnCode = isDynamic(table) ? "col.getColumnName()" : null;
            String valueCode = component.getValueCode("row", columnCode, "col.getValue()");
            String varName = Renderers.camelCase(compDesc.getComponentName());
            if (component.isMultiple()) {
                _("Iterable<", compDesc.getType().getJavaObjectClassName(), "> ", varName, "Iterable = ", valueCode, ";");
            } else {
                _(compDesc.getType().getJavaClassName(), " ", varName, " = ", valueCode, ";");
            }
            return varName;
        }

        private void renderDynamicTouch() {
            _("@Override");
            _("public void touch(Multimap<", Row, ", ", Column, "> values) {"); {
                _("Multimap<", Row, ", ", ColumnValue, "> currentValues = get(values);");
                _("put(currentValues);");
                _("Multimap<", Row, ", ", Column, "> toDelete = HashMultimap.create(values);");
                _("for (Map.Entry<", Row, ", ", ColumnValue, "> e : currentValues.entries()) {"); {
                    _("toDelete.remove(e.getKey(), e.getValue().getColumnName());");
                } _("}");
                _("delete(toDelete);");
            } _("}");
        }

        private void renderGetRowColumns(boolean isDynamic) {
            _("@Override");
            _("public List<", ColumnValue, "> getRowColumns(", Row, " row) {"); {
                _("return getRowColumns(row, ColumnSelection.all());");
            } _("}");
            _();
            _("@Override");
            _("public List<", ColumnValue, "> getRowColumns(", Row, " row, ColumnSelection columns) {"); {
                _("byte[] bytes = row.persistToBytes();");
                _("RowResult<byte[]> rowResult = t.getRows(tableName, ImmutableSet.of(bytes), columns).get(bytes);");
                _("if (rowResult == null) {"); {
                    _("return ImmutableList.of();");
                } _("} else {"); {
                    _("List<", ColumnValue, "> ret = Lists.newArrayListWithCapacity(rowResult.getColumns().size());");
                    _("for (Entry<byte[], byte[]> e : rowResult.getColumns().entrySet()) {"); {
                        if (isDynamic) {
                            _(Column, " col = ", Column, ".BYTES_HYDRATOR.hydrateFromBytes(e.getKey());");
                            _(table.getColumns().getDynamicColumn().getValue().getJavaObjectTypeName(), " val = ", ColumnValue, ".hydrateValue(e.getValue());");
                            _("ret.add(", ColumnValue, ".of(col, val));");
                        } else {
                            _("ret.add(shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));");
                        }
                    } _("}");
                    _("return ret;");
                } _("}");
            } _("}");
        }

        private void renderColumnSelection(boolean isDynamic) {
            _("public static ColumnSelection getColumnSelection(Collection<", Column, "> cols) {");
                _("return ColumnSelection.create(Collections2.transform(cols, ", isDynamic ? "Persistables.persistToBytesFunction()" : Column + ".toShortName()", "));");
            _("}");
            _();
            _("public static ColumnSelection getColumnSelection(", Column, "... cols) {");
                _("return getColumnSelection(Arrays.asList(cols));");
            _("}");
        }

        private void renderShortNameToHydrator() {
            _("private static final Map<String, Hydrator<? extends ", ColumnValue, ">> shortNameToHydrator =");
            _("        ImmutableMap.<String, Hydrator<? extends ", ColumnValue, ">>builder()");
            for (NamedColumnDescription col : table.getColumns().getNamedColumns()) {
                _("            .put(", ColumnRenderers.short_name(col), ", ", ColumnRenderers.VarName(col), ".BYTES_HYDRATOR)");
            }
            _("            .build();");
        }

        private void renderNamedGetColumn(NamedColumnDescription col) {
            _("public Map<", Row, ", ", ColumnRenderers.TypeName(col), "> get", ColumnRenderers.VarName(col), "s(Collection<", Row, "> rows) {"); {
                _("Map<Cell, ", Row, "> cells = Maps.newHashMapWithExpectedSize(rows.size());");
                _("for (", Row, " row : rows) {"); {
                    _("cells.put(Cell.create(row.persistToBytes(), PtBytes.toCachedBytes(", ColumnRenderers.short_name(col), ")), row);");
                } _("}");
                _("Map<Cell, byte[]> results = t.get(tableName, cells.keySet());");
                _("Map<", Row, ", ", ColumnRenderers.TypeName(col), "> ret = Maps.newHashMapWithExpectedSize(results.size());");
                _("for (Entry<Cell, byte[]> e : results.entrySet()) {"); {
                    _(ColumnRenderers.TypeName(col), " val = ", ColumnRenderers.VarName(col), ".BYTES_HYDRATOR.hydrateFromBytes(e.getValue()).getValue();");
                    _("ret.put(cells.get(e.getKey()), val);");
                } _("}");
                _("return ret;");
            } _("}");
        }

        private void renderNamedPutColumn(NamedColumnDescription col) {
            String params = isExpiring(table) ? ", long duration, TimeUnit unit" : "";
            String args = isExpiring(table) ? ", duration, unit" : "";
            String Value = col.getValue().getJavaObjectTypeName();
            _("public void put", ColumnRenderers.VarName(col), "(", Row, " row, ", Value, " value", params, ") {"); {
                _("put(ImmutableMultimap.of(row, ", ColumnRenderers.VarName(col), ".of(value))", args, ");");
            } _("}");
            _();
            _("public void put", ColumnRenderers.VarName(col), "(Map<", Row, ", ", Value, "> map", params, ") {"); {
                _("Map<", Row, ", ", ColumnValue, "> toPut = Maps.newHashMapWithExpectedSize(map.size());");
                _("for (Entry<", Row, ", ", Value, "> e : map.entrySet()) {"); {
                    _("toPut.put(e.getKey(), ", ColumnRenderers.VarName(col), ".of(e.getValue()));");
                } _("}");
                _("put(Multimaps.forMap(toPut)", args, ");");
            } _("}");
        }

        private void renderNamedGetAffectedCells() {
            _("private Multimap<", Row, ", ", ColumnValue, "> getAffectedCells(Multimap<", Row, ", ? extends ", ColumnValue, "> rows) {"); {
                _("Multimap<", Row, ", ", ColumnValue, "> oldData = getRowsMultimap(rows.keySet());");
                _("Multimap<", Row, ", ", ColumnValue, "> cellsAffected = ArrayListMultimap.create();");
                _("for (", Row, " row : oldData.keySet()) {"); {
                    _("Set<String> columns = new HashSet<String>();");
                    _("for (", ColumnValue, " v : rows.get(row)) {"); {
                        _("columns.add(v.getColumnName());");
                    } _("}");
                    _("for (", ColumnValue, " v : oldData.get(row)) {"); {
                        _("if (columns.contains(v.getColumnName())) {"); {
                            _("cellsAffected.put(row, v);");
                        } _("}");
                    } _("}");
                } _("}");
                _("return cellsAffected;");
            } _("}");
        }

        private void renderNamedPut() {
            String params = isExpiring(table) ? ", long duration, TimeUnit unit" : "";
            String args = isExpiring(table) ? ", duration, unit" : "";
            _("@Override");
            _("public void put(Multimap<", Row, ", ? extends ", ColumnValue, "> rows", params, ") {"); {
                _("t.useTable(tableName, this);");

                if (!cellReferencingIndices.isEmpty()) {
                    _("Multimap<", Row, ", ", ColumnValue, "> affectedCells = getAffectedCells(rows);");
                    for (IndexMetadata index : cellReferencingIndices) {
                        String indexName = Renderers.getIndexTableName(index);
                        _("delete", indexName, "(affectedCells);");
                    }
                }

                if (!indices.isEmpty()) {
                    _("for (Entry<", Row, ", ? extends ", ColumnValue, "> e : rows.entries()) {"); {
                        for (IndexMetadata index : indices) {
                            renderIndexPut(index);
                        }
                    } _("}");
                }
                _("t.put(tableName, ColumnValues.toCellValues(rows", args, "));");
                _("for (", Trigger, " trigger : triggers) {"); {
                    _("trigger.put", tableName, "(rows);");
                } _("}");
            } _("}");
        }

        private void renderCellReferencingIndexDelete(IndexMetadata index) {
            String indexName = Renderers.getIndexTableName(index);
            _("private void delete", indexName, "(Multimap<", Row, ", ", ColumnValue, "> result) {"); {
                List<String> rowArgumentNames = Lists.newArrayList();
                List<String> colArgumentNames = Lists.newArrayList();
                List<TypeAndName> iterableArgNames = Lists.newArrayList();

                boolean hasCol = index.getColumnNameToAccessData() != null;
                String columnClass = hasCol ? Renderers.CamelCase(index.getColumnNameToAccessData()) : null;

                _("ImmutableSet.Builder<Cell> indexCells = ImmutableSet.builder();");

                _("for (Entry<", Row, ", ", ColumnValue, "> e : result.entries()) {"); {

                    if (columnClass != null) {
                        _("if (e.getValue() instanceof ", columnClass, ") ");
                    }
                    __("{"); {
                        if (columnClass != null) {
                            _(columnClass, " col = (", columnClass, ") e.getValue();");
                        }
                        if (index.getIndexCondition() != null) {
                            _("if (" + index.getIndexCondition().getValueCode("col.getValue()") + ") ");
                        }
                        __("{"); {
                            _(Row, " row = e.getKey();");

                            for (IndexComponent component : index.getRowComponents()) {
                                String varName = renderIndexComponent(component);
                                rowArgumentNames.add(varName);
                                if (component.isMultiple()) {
                                    iterableArgNames.add(new TypeAndName(component.getRowKeyDescription().getType().getJavaClassName(), varName));
                                }
                            }

                            Collections.addAll(colArgumentNames, "row.persistToBytes()", "e.getValue().persistColumnName()");

                            for (IndexComponent component : index.getColumnComponents()) {
                                String varName = renderIndexComponent(component);
                                colArgumentNames.add(varName);
                                if (component.isMultiple()) {
                                    iterableArgNames.add(new TypeAndName(component.getRowKeyDescription().getType().getJavaClassName(), varName));
                                }
                            }

                            for (TypeAndName iterableArg : iterableArgNames) {
                                _("for (", iterableArg.toString(), " : ", iterableArg.name, "Iterable) {");
                            }

                            _(indexName, "Table.", indexName, "Row indexRow = ", indexName, "Table.", indexName, "Row.of(", Joiner.on(", ").join(rowArgumentNames), ");");
                            _(indexName, "Table.", indexName, "Column indexCol = ", indexName, "Table.", indexName, "Column.of(", Joiner.on(", ").join(colArgumentNames), ");");
                            _("indexCells.add(Cell.create(indexRow.persistToBytes(), indexCol.persistToBytes()));");

                            for (int i = 0 ; i < iterableArgNames.size() ; i++) {
                                _("}");
                            }
                        } _("}");
                    } _("}");
                } _("}");
                _("t.delete(\"", Schemas.getFullTableName(index.getIndexName(), namespace), "\", indexCells.build());");
            } _("}");
        }

        private void renderNamedDeleteColumn(NamedColumnDescription col) {
            Collection<IndexMetadata> columnIndices = Lists.newArrayList();
            for (IndexMetadata index : cellReferencingIndices) {
                if (col.getLongName().equals(index.getColumnNameToAccessData())) {
                    columnIndices.add(index);
                }
            }
            _("public void delete", ColumnRenderers.VarName(col), "(", Row, " row) {"); {
                _("delete", ColumnRenderers.VarName(col), "(ImmutableSet.of(row));");
            } _("}");
            _();
            _("public void delete", ColumnRenderers.VarName(col), "(Iterable<", Row, "> rows) {"); {
                _("byte[] col = PtBytes.toCachedBytes(", ColumnRenderers.short_name(col), ");");
                _("Set<Cell> cells = Cells.cellsWithConstantColumn(Persistables.persistAll(rows), col);");
                if (!columnIndices.isEmpty()) {
                    _("Map<Cell, byte[]> results = t.get(tableName, cells);");
                    for (IndexMetadata index : columnIndices) {
                        _("delete", Renderers.getIndexTableName(index), "Raw(results);");
                    }
                }
                _("t.delete(tableName, cells);");
            } _("}");
            for (IndexMetadata index : columnIndices) {
                _();
                renderNamedIndexDeleteRaw(col, index);
            }
        }

        private void renderNamedIndexDeleteRaw(NamedColumnDescription col, IndexMetadata index) {
            String indexName = Renderers.getIndexTableName(index);
            String NamedColumn = Renderers.CamelCase(col.getLongName());
            _("private void delete", indexName, "Raw(Map<Cell, byte[]> results) {"); {
                List<String> rowArgumentNames = Lists.newArrayList();
                List<String> colArgumentNames = Lists.newArrayList();
                List<TypeAndName> iterableArgNames = Lists.newArrayList();

                _("ImmutableSet.Builder<Cell> indexCells = ImmutableSet.builder();");
                _("for (Entry<Cell, byte[]> result : results.entrySet()) {"); {
                    _(NamedColumn, " col = (", NamedColumn, ") shortNameToHydrator.get(", ColumnRenderers.short_name(col), ").hydrateFromBytes(result.getValue());");

                    _(Row, " row = ", Row, ".BYTES_HYDRATOR.hydrateFromBytes(result.getKey().getRowName());");

                    for (IndexComponent component : index.getRowComponents()) {
                        String varName = renderIndexComponent(component);
                        rowArgumentNames.add(varName);
                        if (component.isMultiple()) {
                            iterableArgNames.add(new TypeAndName(component.getRowKeyDescription().getType().getJavaClassName(), varName));
                        }
                    }
                    Collections.addAll(colArgumentNames, "row.persistToBytes()", "col.persistColumnName()");

                    for (IndexComponent component : index.getColumnComponents()) {
                        String varName = renderIndexComponent(component);
                        colArgumentNames.add(varName);
                        if (component.isMultiple()) {
                            iterableArgNames.add(new TypeAndName(component.getRowKeyDescription().getType().getJavaClassName(), varName));
                        }
                    }

                    for (TypeAndName iterableArg : iterableArgNames) {
                        _("for (", iterableArg.toString(), " : ", iterableArg.name, "Iterable) {");
                    }

                    _(indexName, "Table.", indexName, "Row indexRow = ", indexName, "Table.", indexName, "Row.of(", Joiner.on(", ").join(rowArgumentNames), ");");
                    _(indexName, "Table.", indexName, "Column indexCol = ", indexName, "Table.", indexName, "Column.of(", Joiner.on(", ").join(colArgumentNames), ");");
                    _("indexCells.add(Cell.create(indexRow.persistToBytes(), indexCol.persistToBytes()));");

                    for (int i = 0 ; i < iterableArgNames.size() ; i++) {
                        _("}");
                    }
                }  _("}");
                _("t.delete(\"", Schemas.getFullTableName(index.getIndexName(), namespace), "\", indexCells.build());");
            } _("}");
        }

        private void renderNamedDelete() {
            _("@Override");
            _("public void delete(", Row, " row) {"); {
                _("delete(ImmutableSet.of(row));");
            } _("}");
            _();
            _("@Override");
            _("public void delete(Iterable<", Row, "> rows) {"); {

                if (!cellReferencingIndices.isEmpty()) {
                    _("Multimap<", Row, ", ", ColumnValue, "> result = getRowsMultimap(rows);");
                    for (IndexMetadata index : cellReferencingIndices) {
                        _("delete", Renderers.getIndexTableName(index), "(result);");
                    }
                }

                _("List<byte[]> rowBytes = Persistables.persistAll(rows);");
                _("ImmutableSet.Builder<Cell> cells = ImmutableSet.builder();");
                for (NamedColumnDescription col : ColumnRenderers.namedColumns(table)) {
                    _("cells.addAll(Cells.cellsWithConstantColumn(rowBytes, PtBytes.toCachedBytes(", ColumnRenderers.short_name(col), ")));");
                }
                _("t.delete(tableName, cells.build());");
            } _("}");
        }

        private void renderGetRange() {
            _("public BatchingVisitableView<", RowResult, "> getRange(RangeRequest range) {"); {
                _("if (range.getColumnNames().isEmpty()) {"); {
                    _("range = range.getBuilder().retainColumns(ColumnSelection.all()).build();");
                } _("}");
                _("return BatchingVisitables.transform(t.getRange(tableName, range), new Function<RowResult<byte[]>, ", RowResult, ">() {"); {
                    _("@Override");
                    _("public ", RowResult, " apply(RowResult<byte[]> input) {"); {
                        _("return ", RowResult, ".of(input);");
                    } _("}");
                } _("});");
            } _("}");
        }

        private void renderGetRanges() {
            _("public IterableView<BatchingVisitable<", RowResult, ">> getRanges(Iterable<RangeRequest> ranges) {"); {
                _("Iterable<BatchingVisitable<RowResult<byte[]>>> rangeResults = t.getRanges(tableName, ranges);");
                _("return IterableView.of(Iterables.transform(rangeResults,");
                _("        new Function<BatchingVisitable<RowResult<byte[]>>, BatchingVisitable<", RowResult, ">>() {"); {
                    _("@Override");
                    _("public BatchingVisitable<", RowResult, "> apply(BatchingVisitable<RowResult<byte[]>> visitable) {"); {
                        _("return BatchingVisitables.transform(visitable, new Function<RowResult<byte[]>, ", RowResult, ">() {"); {
                            _("@Override");
                            _("public ", RowResult, " apply(RowResult<byte[]> row) {"); {
                                _("return ", RowResult, ".of(row);");
                            } _("}");
                        } _("});");
                    } _("}");
                } _("}));");
            } _("}");
        }

        private void renderDeleteRange() {
            _("public void deleteRange(RangeRequest range) {"); {
                _("deleteRanges(ImmutableSet.of(range));");
            } _("}");
        }

        private void renderDynamicDeleteRanges() {
            _("public void deleteRanges(Iterable<RangeRequest> ranges) {"); {
                _("BatchingVisitables.concat(getRanges(ranges)).batchAccept(1000, new AbortingVisitor<List<", RowResult, ">, RuntimeException>() {"); {
                    _("@Override");
                    _("public boolean visit(List<", RowResult, "> rowResults) {"); {
                        _("Multimap<", Row, ", ", Column, "> toRemove = HashMultimap.create();");
                        _("for (", RowResult, " rowResult : rowResults) {"); {
                            _("for (", ColumnValue, " columnValue : rowResult.getColumnValues()) {"); {
                                _("toRemove.put(rowResult.getRowName(), columnValue.getColumnName());");
                            } _("}");
                        } _("}");
                        _("delete(toRemove);");
                        _("return true;");
                    } _("}");
                } _("});");
            } _("}");
        }

        private void renderNamedDeleteRanges() {
            _("public void deleteRanges(Iterable<RangeRequest> ranges) {"); {
                _("BatchingVisitables.concat(getRanges(ranges))");
                _("                  .transform(", RowResult, ".getRowNameFun())");
                _("                  .batchAccept(1000, new AbortingVisitor<List<", Row, ">, RuntimeException>() {"); {
                    _("@Override");
                    _("public boolean visit(List<", Row, "> rows) {"); {
                        _("delete(rows);");
                        _("return true;");
                    } _("}");
                } _("});");
            } _("}");
        }

        private void renderGetAllRowsUnordered() {
            _("public BatchingVisitableView<", RowResult, "> getAllRowsUnordered() {"); {
                _("return getAllRowsUnordered(ColumnSelection.all());");
            } _("}");
            _();
            _("public BatchingVisitableView<", RowResult, "> getAllRowsUnordered(ColumnSelection columns) {"); {
                _("return BatchingVisitables.transform(t.getRange(tableName, RangeRequest.builder().retainColumns(columns).build()),");
                _("        new Function<RowResult<byte[]>, ", RowResult, ">() {"); {
                    _("@Override");
                    _("public ", RowResult, " apply(RowResult<byte[]> input) {"); {
                        _("return ", RowResult, ".of(input);");
                    } _("}");
                } _("});");
            } _("}");
        }

        private void renderNamedGetRow() {
            _("@Override");
            _("public Optional<", RowResult, "> getRow(", Row, " row) {"); {
                _("return getRow(row, ColumnSelection.all());");
            } _("}");
            _();
            _("@Override");
            _("public Optional<", RowResult, "> getRow(", Row, " row, ColumnSelection columns) {"); {
                _("byte[] bytes = row.persistToBytes();");
                _("RowResult<byte[]> rowResult = t.getRows(tableName, ImmutableSet.of(bytes), columns).get(bytes);");
                _("if (rowResult == null) {"); {
                    _("return Optional.absent();");
                } _("} else {"); {
                    _("return Optional.of(", RowResult, ".of(rowResult));");
                } _("}");
            } _("}");
        }

        private void renderNamedGetRows() {
            _("@Override");
            _("public List<", RowResult, "> getRows(Iterable<", Row, "> rows) {"); {
                _("return getRows(rows, ColumnSelection.all());");
            } _("}");
            _();
            _("@Override");
            _("public List<", RowResult, "> getRows(Iterable<", Row, "> rows, ColumnSelection columns) {"); {
                _("SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableName, Persistables.persistAll(rows), columns);");
                _("List<", RowResult, "> rowResults = Lists.newArrayListWithCapacity(results.size());");
                _("for (RowResult<byte[]> row : results.values()) {"); {
                    _("rowResults.add(", RowResult, ".of(row));");
                } _("}");
                _("return rowResults;");
            } _("}");
            _();
            _("@Override");
            _("public List<", RowResult, "> getAsyncRows(Iterable<", Row, "> rows, ExecutorService exec) {"); {
                _("return getAsyncRows(rows, ColumnSelection.all(), exec);");
            } _("}");
            _();
            _("@Override");
            _("public List<", RowResult, "> getAsyncRows(final Iterable<", Row, "> rows, final ColumnSelection columns, ExecutorService exec) {"); {
                _("Callable<List<", RowResult, ">> c =");
                _("        new Callable<List<", RowResult, ">>() {"); {
                    _("@Override");
                    _("public List<", RowResult, "> call() {"); {
                        _("return getRows(rows, columns);");
                    } _("}");
                } _("};");
                _("return AsyncProxy.create(exec.submit(c), List.class);");
            } _("}");
        }

        private void renderDynamicGet() {
            _("@Override");
            _("public Multimap<", Row, ", ", ColumnValue, "> get(Multimap<", Row, ", ", Column, "> cells) {"); {
                _("Set<Cell> rawCells = ColumnValues.toCells(cells);");
                _("Map<Cell, byte[]> rawResults = t.get(tableName, rawCells);");
                _("Multimap<", Row, ", ", ColumnValue, "> rowMap = HashMultimap.create();");
                _("for (Entry<Cell, byte[]> e : rawResults.entrySet()) {");
                    _("if (e.getValue().length > 0) {");
                        _(Row, " row = ", Row, ".BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getRowName());");
                        _(Column, " col = ", Column, ".BYTES_HYDRATOR.hydrateFromBytes(e.getKey().getColumnName());");
                        _(table.getColumns().getDynamicColumn().getValue().getJavaObjectTypeName(), " val = ", ColumnValue, ".hydrateValue(e.getValue());");
                        _("rowMap.put(row, ", ColumnValue, ".of(col, val));");
                    _("}");
                _("}");
                _("return rowMap;");
            } _("}");
            _();
            _("@Override");
            _("public Multimap<", Row, ", ", ColumnValue, "> getAsync(final Multimap<", Row, ", ", Column, "> cells, ExecutorService exec) {"); {
                _("Callable<Multimap<", Row, ", ", ColumnValue, ">> c =");
                _("        new Callable<Multimap<", Row, ", ", ColumnValue, ">>() {"); {
                    _("@Override");
                    _("public Multimap<", Row, ", ", ColumnValue, "> call() {"); {
                        _("return get(cells);");
                    } _("}");
                } _("};");
                _("return AsyncProxy.create(exec.submit(c), Multimap.class);");
            } _("}");
        }

        private void renderGetRowsMultimap(boolean isDynamic) {
            _("@Override");
            _("public Multimap<", Row, ", ", ColumnValue, "> getRowsMultimap(Iterable<", Row, "> rows) {"); {
                _("return getRowsMultimapInternal(rows, ColumnSelection.all());");
            } _("}");
            _();
            _("@Override");
            _("public Multimap<", Row, ", ", ColumnValue, "> getRowsMultimap(Iterable<", Row, "> rows, ColumnSelection columns) {"); {
                _("return getRowsMultimapInternal(rows, columns);");
            } _("}");
            _();
            _("@Override");
            _("public Multimap<", Row, ", ", ColumnValue, "> getAsyncRowsMultimap(Iterable<", Row, "> rows, ExecutorService exec) {"); {
                _("return getAsyncRowsMultimap(rows, ColumnSelection.all(), exec);");
            } _("}");
            _();
            _("@Override");
            _("public Multimap<", Row, ", ", ColumnValue, "> getAsyncRowsMultimap(final Iterable<", Row, "> rows, final ColumnSelection columns, ExecutorService exec) {"); {
                _("Callable<Multimap<", Row, ", ", ColumnValue, ">> c =");
                _("        new Callable<Multimap<", Row, ", ", ColumnValue, ">>() {"); {
                    _("@Override");
                    _("public Multimap<", Row, ", ", ColumnValue, "> call() {"); {
                        _("return getRowsMultimapInternal(rows, columns);");
                    } _("}");
                } _("};");
                _("return AsyncProxy.create(exec.submit(c), Multimap.class);");
            } _("}");
            _();
            _("private Multimap<", Row, ", ", ColumnValue, "> getRowsMultimapInternal(Iterable<", Row, "> rows, ColumnSelection columns) {"); {
                _("SortedMap<byte[], RowResult<byte[]>> results = t.getRows(tableName, Persistables.persistAll(rows), columns);");
                _("return getRowMapFromRowResults(results.values());");
            } _("}");
            _();
            _("private static Multimap<", Row, ", ", ColumnValue, "> getRowMapFromRowResults(Collection<RowResult<byte[]>> rowResults) {"); {
                _("Multimap<", Row, ", ", ColumnValue, "> rowMap = HashMultimap.create();");
                _("for (RowResult<byte[]> result : rowResults) {"); {
                    _(Row, " row = ", Row, ".BYTES_HYDRATOR.hydrateFromBytes(result.getRowName());");
                    _("for (Entry<byte[], byte[]> e : result.getColumns().entrySet()) {"); {
                        if (isDynamic) {
                            _(Column, " col = ", Column, ".BYTES_HYDRATOR.hydrateFromBytes(e.getKey());");
                            _(table.getColumns().getDynamicColumn().getValue().getJavaObjectTypeName(), " val = ", ColumnValue, ".hydrateValue(e.getValue());");
                            _("rowMap.put(row, ", ColumnValue, ".of(col, val));");
                        } else {
                            _("rowMap.put(row, shortNameToHydrator.get(PtBytes.toString(e.getKey())).hydrateFromBytes(e.getValue()));");
                        }
                    } _("}");
                } _("}");
                _("return rowMap;");
            } _("}");
        }

        private void renderFindConstraintFailures() {
            _("@Override");
            _("public List<String> findConstraintFailures(Map<Cell, byte[]> writes,");
            _("                                           ConstraintCheckingTransaction transaction,");
            _("                                           AtlasDbConstraintCheckingMode constraintCheckingMode) {"); {
                _("return ImmutableList.of();");
            } _("}");
            _();
            _("@Override");
            _("public List<String> findConstraintFailuresNoRead(Map<Cell, byte[]> writes,");
            _("                                                 AtlasDbConstraintCheckingMode constraintCheckingMode) {"); {
                _("return ImmutableList.of();");
            } _("}");
        }

        private void renderAdd() {
            String params = isExpiring(table) ? ", long duration, TimeUnit unit" : "";
            String args = isExpiring(table) ? ", duration, unit" : "";
            _("@Override");
            _("public void add(", Row, " row", params, ") {"); {
                _("add(ImmutableSet.of(row)", args, ");");
            } _("}");
            _();
            _("@Override");
            _("public void add(Set<", Row, "> rows", params, ") {"); {
                _("Map<", Row, ", ", ColumnValue, "> map = Maps.newHashMapWithExpectedSize(rows.size());");
                _(ColumnValue, " col = Exists.of(0L);");
                _("for (", Row, " row : rows) {"); {
                    _("map.put(row, col);");
                } _("}");
                _("put(Multimaps.forMap(map)", args, ");");
            } _("}");
        }

        private void renderClassHash() {
            byte[] hash = getHash();
            _("static String __CLASS_HASH = \"", BaseEncoding.base64().encode(hash), "\";");
        }
    }

    // ================================================== //
    // Nothing after this point should have side-effects. //
    // ================================================== //

    private static boolean isNamedSet(TableMetadata table) {
        Set<NamedColumnDescription> namedColumns = table.getColumns().getNamedColumns();
        return namedColumns != null && namedColumns.size() == 1 && Iterables.getOnlyElement(namedColumns).getLongName().equals("exists");
    }

    private static boolean isDynamic(TableMetadata table) {
        return table.getColumns().hasDynamicColumns();
    }

    private static boolean isExpiring(TableMetadata table) {
        return table.getExpirationStrategy() == ExpirationStrategy.INDIVIDUALLY_SPECIFIED;
    }

    private static Collection<IndexMetadata> getCellReferencingIndices(SortedSet<IndexMetadata> indices) {
        return Collections2.filter(indices, new Predicate<IndexMetadata>() {
            @Override
            public boolean apply(IndexMetadata index) {
                return index.getIndexType() == IndexType.CELL_REFERENCING;
            }
        });
    }

    private static final Class<?>[] IMPORTS = {
        Set.class,
        List.class,
        Map.class,
        SortedMap.class,
        Callable.class,
        ExecutorService.class,
        Multimap.class,
        Multimaps.class,
        Collection.class,
        Function.class,
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
        AsyncProxy.class,
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
        EnumSet.class,
        Descending.class,
        AbortingVisitor.class,
        AbortingVisitors.class,
        AssertUtils.class,
        AtlasDbConstraintCheckingMode.class,
        ConstraintCheckingTransaction.class,
        AtlasDbDynamicMutableExpiringTable.class,
        AtlasDbDynamicMutablePersistentTable.class,
        AtlasDbNamedPersistentSet.class,
        AtlasDbNamedExpiringSet.class,
        AtlasDbMutablePersistentTable.class,
        AtlasDbMutableExpiringTable.class,
        AtlasDbNamedMutableTable.class,
        ColumnSelection.class,
        Joiner.class,
        Entry.class,
        Iterator.class,
        Iterables.class,
        Supplier.class,
        InvalidProtocolBufferException.class,
        Throwables.class,
        ImmutableList.class,
        UnsignedBytes.class,
        Optional.class,
        Collections2.class,
        Arrays.class,
        Bytes.class,
        TypedRowResult.class,
        TimeUnit.class,
        CompressionUtils.class,
        Compression.class
    };
}
