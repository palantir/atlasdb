/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import static com.palantir.atlasdb.table.description.render.ColumnRenderers.VarName;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Generated;
import javax.lang.model.element.Modifier;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.generation.ColumnValues;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.common.base.BatchingVisitableView;
import com.palantir.common.persist.Persistables;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

public class TableRendererV2 {
    private final String packageName;
    private final Namespace namespace;

    public TableRendererV2(String packageName, Namespace namespace) {
        Preconditions.checkNotNull(packageName);
        Preconditions.checkNotNull(namespace);
        this.packageName = packageName;
        this.namespace = namespace;
    }

    public String getClassName(String rawTableName, TableDefinition table) {
        return Renderers.getClassTableName(rawTableName, table) + "SimpleTable";
    }

    public String render(String rawTableName, TableDefinition table) {
        return new ClassRenderer(rawTableName, table).render();
    }

    private class ClassRenderer {
        private final TableMetadata tableMetadata;
        private final String rawTableName;
        private final String tableName;

        private final String tableClassName;
        private final ClassName tableType;

        private final String rowClassName;
        private final ClassName rowType;

        private final String rowResultClassName;
        private final ClassName rowResultType;

        private final String simpleTableName;
        private final ClassName simpleTableType;

        ClassRenderer(String rawTableName, TableDefinition table) {
            Preconditions.checkArgument(
                    Schemas.isTableNameValid(rawTableName),
                    "Invalid table name " + rawTableName);
            this.rawTableName = rawTableName;
            this.tableName = Renderers.getClassTableName(rawTableName, table);
            this.tableMetadata = table.toTableMetadata();

            this.simpleTableName = this.tableName + "SimpleTable";
            this.tableClassName = this.tableName + "Table";
            this.rowClassName = this.tableName + "Row";
            this.rowResultClassName = this.tableName + "RowResult";

            this.tableType = ClassName.get(packageName, this.tableClassName);
            this.simpleTableType = ClassName.get(packageName, this.simpleTableName);

            this.rowType = this.tableType.nestedClass(this.rowClassName);
            this.rowResultType = this.tableType.nestedClass(this.rowResultClassName);
        }

        public String render() {
            JavaFile javaFile = JavaFile.builder(packageName, this.run())
                    .indent("    ")
                    .build();

            return javaFile.toString();
        }

        private TypeSpec run() {
            TypeSpec.Builder tableBuilder = TypeSpec.classBuilder(this.simpleTableName)
                    .addAnnotation(AnnotationSpec.builder(Generated.class)
                            .addMember("value", "$S", TableRendererV2.class.getName())
                            .build())
                    .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class)
                            .addMember("value", "$S", "all")
                            .build())
                    .addModifiers(Modifier.PUBLIC, Modifier.FINAL);

            buildFactory(this::getFields, tableBuilder::addField);
            buildFactory(this::getConstructors, tableBuilder::addMethod);
            buildFactory(this::getStaticFactories, tableBuilder::addMethod);
            buildFactory(this::getMethods, tableBuilder::addMethod);

            return tableBuilder.build();
        }

        <T> void buildFactory(Supplier<Collection<T>> supplier, Consumer<T> addToFactory) {
            for (T entry : supplier.get()) {
                addToFactory.accept(entry);
            }
        }

        private List<FieldSpec> getFields() {
            ArrayList<FieldSpec> results = new ArrayList<>();
            results.add(FieldSpec.builder(tableType, "tableV1")
                    .addModifiers(Modifier.PRIVATE)
                    .build());
            results.add(FieldSpec.builder(Transaction.class, "t")
                    .addModifiers(Modifier.PRIVATE, Modifier.FINAL)
                    .build());

            results.add(FieldSpec.builder(String.class, "rawTableName")
                    .addModifiers(Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
                    .initializer("$S", rawTableName)
                    .build());
            results.add(FieldSpec.builder(TableReference.class, "tableRef")
                    .addModifiers(Modifier.PRIVATE, Modifier.FINAL)
                    .build());
            return results;
        }

        private List<MethodSpec> getConstructors() {
            ArrayList<MethodSpec> results = new ArrayList<>();
            results.add(MethodSpec.constructorBuilder()
                    .addModifiers(Modifier.PRIVATE)
                    .addParameter(Transaction.class, "t")
                    .addParameter(Namespace.class, "namespace")
                    .addStatement("this.tableRef = TableReference.create(namespace, rawTableName)",
                            TableReference.class)
                    .addStatement("this.t = t")
                    .addStatement("this.tableV1 = $T.of(t, namespace)", tableType)
                    .build());
            return results;
        }

        private List<MethodSpec> getStaticFactories() {
            ArrayList<MethodSpec> results = new ArrayList<>();
            results.add(MethodSpec.methodBuilder("of")
                    .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                    .addParameter(Transaction.class, "t")
                    .addParameter(Namespace.class, "namespace")
                    .returns(simpleTableType)
                    .addStatement("return new $T($L, $L)",
                            simpleTableType, "t", "namespace")
                    .build());

            return results;
        }

        private List<MethodSpec> getMethods() {
            ArrayList<MethodSpec> results = new ArrayList<>();
            results.add(renderGetRawTableName());
            results.add(renderGetTableRef());
            results.add(renderGetTableName());
            results.add(renderGetNamespace());
            results.addAll(renderGet());
            results.add(renderDeleteRow());
            results.addAll(renderPut());
            return results;
        }

        private MethodSpec renderGetRawTableName() {
            return MethodSpec.methodBuilder("getRawTableName")
                    .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                    .addStatement("return rawTableName")
                    .returns(String.class)
                    .build();
        }

        private MethodSpec renderGetTableName() {
            return MethodSpec.methodBuilder("getTableName")
                    .addModifiers(Modifier.PUBLIC)
                    .addStatement("return tableRef.getQualifiedName()")
                    .returns(String.class)
                    .build();
        }

        private MethodSpec renderGetTableRef() {
            return MethodSpec.methodBuilder("getTableRef")
                    .addModifiers(Modifier.PUBLIC)
                    .addStatement("return tableRef")
                    .returns(TableReference.class)
                    .build();
        }

        private MethodSpec renderGetNamespace() {
            return MethodSpec.methodBuilder("getNamespace")
                    .addModifiers(Modifier.PUBLIC)
                    .addStatement("return tableRef.getNamespace()")
                    .returns(Namespace.class)
                    .build();
        }

        private List<MethodSpec> renderGet() {
            ArrayList<MethodSpec> getterResults = new ArrayList<>();
            for (NamedColumnDescription col : ColumnRenderers.namedColumns(tableMetadata)) {
                getterResults.add(renderGetColumn(col));
                if (tableMetadata.getRowMetadata().getRowParts().size() == 1) {
                    getterResults.add(renderGetSeveralRowsColumn(col));
                }
                getterResults.add(renderGetAllRowsColumn(col));
            }

            return getterResults;
        }

        private MethodSpec renderGetColumn(NamedColumnDescription col) {
            MethodSpec.Builder getterBuilder = MethodSpec.methodBuilder("get" + VarName(col))
                    .addModifiers(Modifier.PUBLIC);

            getterBuilder = addParametersFromRowComponents(getterBuilder, tableMetadata);

            getterBuilder.returns(ParameterizedTypeName.get(
                    ClassName.get(Optional.class),
                    ClassName.get(col.getValue().getValueType().getTypeClass())));
            getterBuilder
                    .addStatement("$T row = $T.of($N)", rowType, rowType, getArgumentsFromRowComponents(tableMetadata))
                    .addStatement("byte[] bytes = row.persistToBytes()")
                    .addStatement("$T colSelection = $T.create($T.singletonList($T.toCachedBytes($S)))",
                            ColumnSelection.class, ColumnSelection.class, Collections.class,
                            PtBytes.class, col.getShortName())
                    .addStatement("$T<byte[]> rowResult = t.getRows(tableRef, $T.of(bytes), colSelection).get(bytes)",
                            RowResult.class, ImmutableSet.class)
                    .beginControlFlow("if (rowResult == null)")
                        .addStatement("return $T.empty()", Optional.class)
                    .endControlFlow()
                    .beginControlFlow("else")
                        .addStatement("return $T.of($T.of(rowResult).get$N())",
                                Optional.class, rowResultType, VarName(col))
                    .endControlFlow();

            return getterBuilder.build();
        }

        private MethodSpec renderGetSeveralRowsColumn(NamedColumnDescription col) {
            Preconditions.checkArgument(tableMetadata.getRowMetadata().getRowParts().size() == 1);

            NameComponentDescription rowComponent = tableMetadata.getRowMetadata().getRowParts().get(0);
            MethodSpec.Builder getterBuilder = MethodSpec.methodBuilder("get" + VarName(col) + 's')
                    .addModifiers(Modifier.PUBLIC)
                    .addParameter(
                            ParameterizedTypeName.get(
                                    ClassName.get(Iterable.class),
                                    ClassName.get(rowComponent.getType().getTypeClass())),
                            "rowKeys");

            getterBuilder.returns(ParameterizedTypeName.get(
                    ClassName.get(List.class),
                    ClassName.get(col.getValue().getValueType().getTypeClass())));

            getterBuilder
                    .addStatement("$T colSelection = $T.create($T.singletonList($T.toCachedBytes($S)))",
                            ColumnSelection.class, ColumnSelection.class, Collections.class,
                            PtBytes.class, col.getShortName())
                    .addStatement("$T<$T> rows = $T\n"
                                    + ".newArrayList(rowKeys)\n"
                                    + ".stream()\n"
                                    + ".map($T::of)\n"
                                    + ".collect($T.toList())",
                            List.class, rowType, Lists.class, rowType, Collectors.class)
                    .addStatement("$T<byte[], $T<byte[]>> results = "
                                    + "t.getRows(tableRef, $T.persistAll(rows), colSelection)",
                            SortedMap.class, RowResult.class, Persistables.class)
                    .addStatement("return results\n"
                                    + ".values()\n"
                                    + ".stream()\n"
                                    + ".map(entry -> $T.of(entry).get$N())\n"
                                    + ".collect($T.toList())",
                            rowResultType, VarName(col), Collectors.class);

            return getterBuilder.build();
        }

        private MethodSpec renderGetAllRowsColumn(NamedColumnDescription col) {
            MethodSpec.Builder getterBuilder = MethodSpec.methodBuilder("getAll" + VarName(col) + 's')
                    .addModifiers(Modifier.PUBLIC);

            getterBuilder.returns(ParameterizedTypeName.get(
                    ClassName.get(List.class),
                    ClassName.get(col.getValue().getValueType().getTypeClass())));


            getterBuilder
                    .addStatement("$T colSelection = $T.create($T.singletonList($T.toCachedBytes($S)))",
                            ColumnSelection.class, ColumnSelection.class, Collections.class,
                            PtBytes.class, col.getShortName())
                    .addStatement("return $T.of(t.getRange("
                                        + "tableRef, "
                                        + "$T.builder().retainColumns(colSelection).build()))\n"
                                    + ".immutableCopy()\n"
                                    + ".stream()\n"
                                    + ".map(entry -> $T.of(entry).get$N())\n"
                                    + ".collect($T.toList())",
                            BatchingVisitableView.class, RangeRequest.class, rowResultType,
                            VarName(col), Collectors.class);

            return getterBuilder.build();
        }

        private MethodSpec renderDeleteRow() {
            MethodSpec.Builder deleteRowBuilder = MethodSpec.methodBuilder("deleteRow")
                    .addModifiers(Modifier.PUBLIC);

            deleteRowBuilder = addParametersFromRowComponents(deleteRowBuilder, tableMetadata);

            SortedSet<NamedColumnDescription> namedColumns = ColumnRenderers.namedColumns(tableMetadata);
            deleteRowBuilder
                    .addStatement("$T row = $T.of($N)", rowType, rowType, getArgumentsFromRowComponents(tableMetadata))
                    .addStatement("byte[] rowBytes = row.persistToBytes()", Persistables.class)
                    .addStatement("$T<$T> cells = $T.of()", Set.class, Cell.class, ImmutableSet.class);

            for (NamedColumnDescription col : namedColumns) {
                deleteRowBuilder.addStatement("cells.add($T.create(rowBytes, $T.toCachedBytes($N)))",
                        Cell.class, PtBytes.class, ColumnRenderers.short_name(col));
            }
            deleteRowBuilder.addStatement("t.delete(tableRef, cells)");
            return deleteRowBuilder.build();

        }

        private List<MethodSpec> renderPut() {
            ArrayList<MethodSpec> putResults = new ArrayList<>();
            for (NamedColumnDescription col : ColumnRenderers.namedColumns(tableMetadata)) {
                putResults.add(renderPutColumn(col));
            }

            return putResults;
        }

        private MethodSpec renderPutColumn(NamedColumnDescription col) {
            MethodSpec.Builder putColumnBuilder = MethodSpec.methodBuilder("put" + VarName(col))
                    .addModifiers(Modifier.PUBLIC);

            putColumnBuilder = addParametersFromRowComponents(putColumnBuilder, tableMetadata);

            TypeName columnValueType = tableType.nestedClass(VarName(col));
            putColumnBuilder.addParameter(col.getValue().getValueType().getTypeClass(), col.getLongName());
            putColumnBuilder
                    .addStatement("$T row = $T.of($N)", rowType, rowType, getArgumentsFromRowComponents(tableMetadata))
                    .addStatement("t.put(tableRef, $T.toCellValues($T.of(row, $T.of($N))))",
                            ColumnValues.class, ImmutableMultimap.class, columnValueType, col.getLongName());
            return putColumnBuilder.build();
        }
    }

    private static MethodSpec.Builder addParametersFromRowComponents(
            MethodSpec.Builder methodFactory,
            TableMetadata tableMetadata) {
        for (NameComponentDescription rowPart : getRowComponents(tableMetadata)) {
            methodFactory.addParameter(
                    rowPart.getType().getTypeClass(),
                    rowPart.getComponentName());
        }
        return methodFactory;
    }

    private static String getArgumentsFromRowComponents(TableMetadata tableMetadata) {
        String args = "";
        for (NameComponentDescription rowPart : getRowComponents(tableMetadata)) {
            args += rowPart.getComponentName() + " ,";
        }
        args = args.substring(0, args.length() - 2);
        return args;
    }

    private static List<NameComponentDescription> getRowComponents(TableMetadata tableMetadata) {
        NameMetadataDescription rowMetadata = tableMetadata.getRowMetadata();
        return rowMetadata.getRowParts();
    }
}
