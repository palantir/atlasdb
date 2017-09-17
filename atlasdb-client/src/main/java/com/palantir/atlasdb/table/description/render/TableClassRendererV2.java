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

import static com.palantir.atlasdb.AtlasDbConstants.SCHEMA_V2_TABLE_NAME;
import static com.palantir.atlasdb.table.description.render.ColumnRenderers.VarName;
import static com.palantir.atlasdb.table.description.render.Renderers.CamelCase;
import static com.palantir.atlasdb.table.description.render.Renderers.addParametersFromRowComponents;
import static com.palantir.atlasdb.table.description.render.Renderers.getArgumentsFromRowComponents;
import static com.palantir.atlasdb.table.description.render.Renderers.getColumnTypeClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Generated;
import javax.lang.model.element.Modifier;

import com.google.common.base.Preconditions;
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

public class TableClassRendererV2 {
    private final String packageName;
    private final Namespace namespace;

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

    public TableClassRendererV2(
            String packageName,
            Namespace namespace,
            String rawTableName,
            TableDefinition table) {
        Preconditions.checkArgument(
                Schemas.isTableNameValid(rawTableName),
                "Invalid table name " + rawTableName);
        this.packageName = packageName;
        this.namespace = namespace;
        this.rawTableName = rawTableName;
        this.tableName = Renderers.getClassTableName(rawTableName, table);
        this.tableMetadata = table.toTableMetadata();

        this.simpleTableName = this.tableName + SCHEMA_V2_TABLE_NAME;
        this.tableClassName = this.tableName + "Table";
        this.rowClassName = this.tableName + "Row";
        this.rowResultClassName = this.tableName + "RowResult";

        this.tableType = ClassName.get(packageName, this.tableClassName);
        this.simpleTableType = ClassName.get(packageName, this.simpleTableName);

        this.rowType = this.tableType.nestedClass(this.rowClassName);
        this.rowResultType = this.tableType.nestedClass(this.rowResultClassName);
    }

    public String render() {
        JavaFile javaFile = JavaFile.builder(packageName, this.buildTypeSpec())
                .indent("    ")
                .build();

        return javaFile.toString();
    }

    private TypeSpec buildTypeSpec() {
        TypeSpec.Builder tableBuilder = TypeSpec.classBuilder(this.simpleTableName)
                .addAnnotation(AnnotationSpec.builder(Generated.class)
                        .addMember("value", "$S", TableRendererV2.class.getName())
                        .build())
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class)
                        .addMember("value", "$S", "all")
                        .build())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL);

        getFields().forEach(tableBuilder::addField);
        getConstructors().forEach(tableBuilder::addMethod);
        getStaticFactories().forEach(tableBuilder::addMethod);
        getMethods().forEach(tableBuilder::addMethod);

        return tableBuilder.build();
    }

    private List<FieldSpec> getFields() {
        List<FieldSpec> results = new ArrayList<>();
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
        List<MethodSpec> results = new ArrayList<>();
        results.add(MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PRIVATE)
                .addParameter(Transaction.class, "t")
                .addParameter(Namespace.class, "namespace")
                .addStatement("this.tableRef = $T.create(namespace, rawTableName)", TableReference.class)
                .addStatement("this.t = t")
                .build());
        return results;
    }

    private List<MethodSpec> getStaticFactories() {
        List<MethodSpec> results = new ArrayList<>();
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
        List<MethodSpec> results = new ArrayList<>();
        results.add(renderGetRawTableName());
        results.add(renderGetTableRef());
        results.add(renderGetTableName());
        results.add(renderGetNamespace());
        if (!tableMetadata.getColumns().hasDynamicColumns()) {
            results.addAll(renderNamedGet());
            results.addAll(renderNamedDelete());
            results.addAll(renderNamedPutAndUpdate());
        }
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

    private List<MethodSpec> renderNamedGet() {
        List<MethodSpec> getterResults = new ArrayList<>();
        for (NamedColumnDescription col : ColumnRenderers.namedColumns(tableMetadata)) {
            getterResults.add(renderNamedGetColumn(col));
            if (tableMetadata.getRowMetadata().getRowParts().size() == 1) {
                getterResults.add(renderNamedGetSeveralRows(col));
                getterResults.add(renderNamedGetAllRows(col));
            } else {
                getterResults.add(renderNamedGetSeveralRowsFromRowObjects(col));
                getterResults.add(renderNamedGetAllRowsFromRowObjects(col));
            }
        }

        return getterResults;
    }

    private MethodSpec renderNamedGetColumn(NamedColumnDescription col) {
        MethodSpec.Builder getterBuilder = MethodSpec.methodBuilder("get" + VarName(col))
                .addModifiers(Modifier.PUBLIC);

        getterBuilder = addParametersFromRowComponents(getterBuilder, tableMetadata);

        getterBuilder.returns(ParameterizedTypeName.get(
                ClassName.get(Optional.class),
                ClassName.get(getColumnTypeClass(col))));
        getterBuilder
                .addStatement("$T row = $T.of($L)", rowType, rowType, getArgumentsFromRowComponents(tableMetadata))
                .addStatement("byte[] bytes = row.persistToBytes()")
                .addStatement("$T colSelection = \n"
                                + "$T.create($T.singletonList($T.toCachedBytes($S)))",
                        ColumnSelection.class, ColumnSelection.class, Collections.class,
                        PtBytes.class, col.getShortName())
                .addStatement("$T<byte[]> rowResult = t.getRows(tableRef, $T.of(bytes), colSelection).get(bytes)",
                        RowResult.class, ImmutableSet.class)
                .beginControlFlow("if (rowResult == null)")
                .addStatement("return $T.empty()", Optional.class)
                .endControlFlow()
                .beginControlFlow("else")
                .addStatement("return $T.of($T.of(rowResult).get$L())",
                        Optional.class, rowResultType, VarName(col))
                .endControlFlow();

        return getterBuilder.build();
    }


    private MethodSpec renderNamedGetSeveralRows(NamedColumnDescription col) {
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
                ClassName.get(Map.class),
                ClassName.get(rowComponent.getType().getTypeClass()),
                ClassName.get(getColumnTypeClass(col))));

        getterBuilder
                .addStatement("$T colSelection = \n "
                                + "$T.create($T.singletonList($T.toCachedBytes($S)))",
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
                                + ".map(entry -> $T.of(entry))\n"
                                + ".collect($T.toMap(\n"
                                + "     entry -> entry.getRowName().get$L(), \n"
                                + "     $T::get$L))",
                        rowResultType, Collectors.class, CamelCase(rowComponent.getComponentName()),
                        rowResultType, VarName(col));

        return getterBuilder.build();
    }

    private MethodSpec renderNamedGetSeveralRowsFromRowObjects(NamedColumnDescription col) {
        MethodSpec.Builder getterBuilder = MethodSpec.methodBuilder("get" + VarName(col) + 's')
                .addModifiers(Modifier.PUBLIC)
                .addParameter(ParameterizedTypeName.get(ClassName.get(Iterable.class), rowType), "rowKeys");

        getterBuilder.returns(ParameterizedTypeName.get(
                ClassName.get(Map.class),
                rowType,
                ClassName.get(getColumnTypeClass(col))));

        getterBuilder
                .addStatement("$T colSelection = \n "
                                + "$T.create($T.singletonList($T.toCachedBytes($S)))",
                        ColumnSelection.class, ColumnSelection.class, Collections.class,
                        PtBytes.class, col.getShortName())
                .addStatement("$T<byte[], $T<byte[]>> results = "
                                + "t.getRows(tableRef, $T.persistAll(rowKeys), colSelection)",
                        SortedMap.class, RowResult.class, Persistables.class)
                .addStatement("return results\n"
                                + ".values()\n"
                                + ".stream()\n"
                                + ".map(entry -> $T.of(entry))\n"
                                + ".collect($T.toMap(\n"
                                + "     entry -> entry.getRowName(), \n"
                                + "     $T::get$L))",
                        rowResultType, Collectors.class, rowResultType, VarName(col));

        return getterBuilder.build();
    }


    private MethodSpec renderNamedGetAllRows(NamedColumnDescription col) {
        Preconditions.checkArgument(tableMetadata.getRowMetadata().getRowParts().size() == 1);

        NameComponentDescription rowComponent = tableMetadata.getRowMetadata().getRowParts().get(0);
        MethodSpec.Builder getterBuilder = MethodSpec.methodBuilder("getAll" + VarName(col) + 's')
                .addModifiers(Modifier.PUBLIC);

        getterBuilder.returns(ParameterizedTypeName.get(
                ClassName.get(Map.class),
                ClassName.get(rowComponent.getType().getTypeClass()),
                ClassName.get(getColumnTypeClass(col))));

        getterBuilder
                .addStatement("$T colSelection = \n"
                                + "$T.create($T.singletonList($T.toCachedBytes($S)))",
                        ColumnSelection.class, ColumnSelection.class, Collections.class,
                        PtBytes.class, col.getShortName())
                .addStatement("return $T.of(t.getRange("
                                + "tableRef, \n"
                                + "$T.builder().retainColumns(colSelection).build()))\n"
                                + ".immutableCopy()\n"
                                + ".stream()\n"
                                + ".map(entry -> $T.of(entry))\n"
                                + ".collect($T.toMap(\n"
                                + "     entry -> entry.getRowName().get$L(), \n"
                                + "     $T::get$L))",
                        BatchingVisitableView.class, RangeRequest.class, rowResultType,
                        Collectors.class, CamelCase(rowComponent.getComponentName()), rowResultType, VarName(col));

        return getterBuilder.build();
    }

    private MethodSpec renderNamedGetAllRowsFromRowObjects(NamedColumnDescription col) {
        MethodSpec.Builder getterBuilder = MethodSpec.methodBuilder("getAll" + VarName(col) + 's')
                .addModifiers(Modifier.PUBLIC);

        getterBuilder.returns(ParameterizedTypeName.get(
                ClassName.get(Map.class), rowType, ClassName.get(getColumnTypeClass(col))));

        getterBuilder
                .addStatement("$T colSelection = \n"
                                + "$T.create($T.singletonList($T.toCachedBytes($S)))",
                        ColumnSelection.class, ColumnSelection.class, Collections.class,
                        PtBytes.class, col.getShortName())
                .addStatement("return $T.of(t.getRange("
                                + "tableRef, \n"
                                + "$T.builder().retainColumns(colSelection).build()))\n"
                                + ".immutableCopy()\n"
                                + ".stream()\n"
                                + ".map(entry -> $T.of(entry))\n"
                                + ".collect($T.toMap(\n"
                                + "     entry -> entry.getRowName(), \n"
                                + "     $T::get$L))",
                        BatchingVisitableView.class, RangeRequest.class, rowResultType,
                        Collectors.class, rowResultType, VarName(col));

        return getterBuilder.build();
    }

    private List<MethodSpec> renderNamedDelete() {
        List<MethodSpec> deleteResults = new ArrayList<>();
        deleteResults.add(renderNamedDeleteRow());
        for (NamedColumnDescription col : ColumnRenderers.namedColumns(tableMetadata)) {
            deleteResults.add(renderNamedDeleteColumn(col));
        }

        return deleteResults;
    }

    private MethodSpec renderNamedDeleteRow() {
        MethodSpec.Builder deleteRowBuilder = MethodSpec.methodBuilder("deleteRow")
                .addModifiers(Modifier.PUBLIC);

        deleteRowBuilder = addParametersFromRowComponents(deleteRowBuilder, tableMetadata);

        SortedSet<NamedColumnDescription> namedColumns = ColumnRenderers.namedColumns(tableMetadata);
        deleteRowBuilder
                .addStatement("$T row = $T.of($L)", rowType, rowType, getArgumentsFromRowComponents(tableMetadata))
                .addStatement("byte[] rowBytes = row.persistToBytes()", Persistables.class)
                .addStatement("$T<$T> cells = new $T<>()", Set.class, Cell.class, HashSet.class);

        for (NamedColumnDescription col : namedColumns) {
            deleteRowBuilder.addStatement("cells.add($T.create(rowBytes, $T.toCachedBytes($L)))",
                    Cell.class, PtBytes.class, ColumnRenderers.short_name(col));
        }
        deleteRowBuilder.addStatement("t.delete(tableRef, cells)");
        return deleteRowBuilder.build();

    }

    private MethodSpec renderNamedDeleteColumn(NamedColumnDescription col) {
        MethodSpec.Builder deleteColumnBuilder =
                MethodSpec.methodBuilder("delete" + VarName(col))
                        .addModifiers(Modifier.PUBLIC);

        deleteColumnBuilder = addParametersFromRowComponents(deleteColumnBuilder, tableMetadata);

        return deleteColumnBuilder
                .addStatement("$T row = $T.of($L)", rowType, rowType, getArgumentsFromRowComponents(tableMetadata))
                .addStatement("byte[] rowBytes = row.persistToBytes()", Persistables.class)
                .addStatement("$T<$T> cells = $T.of($T.create(rowBytes, $T.toCachedBytes($L)))",
                        Set.class, Cell.class, ImmutableSet.class,
                        Cell.class, PtBytes.class, ColumnRenderers.short_name(col))
                .addStatement("t.delete(tableRef, cells)")
                .build();
    }

    private List<MethodSpec> renderNamedPutAndUpdate() {
        List<MethodSpec> putAndUpdateResults = new ArrayList<>();
        for (NamedColumnDescription col : ColumnRenderers.namedColumns(tableMetadata)) {
            putAndUpdateResults.add(renderNamedPutColumn(col));
            putAndUpdateResults.add(renderNamedUpdateColumn(col));
        }

        return putAndUpdateResults;
    }

    private MethodSpec renderNamedPutColumn(NamedColumnDescription col) {
        MethodSpec.Builder putColumnBuilder = MethodSpec.methodBuilder("put" + VarName(col))
                .addModifiers(Modifier.PUBLIC);

        putColumnBuilder = addParametersFromRowComponents(putColumnBuilder, tableMetadata);

        TypeName columnValueType = tableType.nestedClass(VarName(col));
        putColumnBuilder.addParameter(getColumnTypeClass(col), col.getLongName());
        putColumnBuilder
                .addStatement("$T row = $T.of($L)", rowType, rowType, getArgumentsFromRowComponents(tableMetadata))
                .addStatement("t.put(tableRef, $T.toCellValues($T.of(row, $T.of($L))))",
                        ColumnValues.class, ImmutableMultimap.class, columnValueType, col.getLongName());
        return putColumnBuilder.build();
    }

    private MethodSpec renderNamedUpdateColumn(NamedColumnDescription col) {
        MethodSpec.Builder updateColumnIfExistsBuilder =
                MethodSpec.methodBuilder("update" + VarName(col))
                        .addModifiers(Modifier.PUBLIC);

        updateColumnIfExistsBuilder = addParametersFromRowComponents(updateColumnIfExistsBuilder, tableMetadata);
        updateColumnIfExistsBuilder.addParameter(ParameterizedTypeName.get(
                ClassName.get(Function.class),
                ClassName.get(getColumnTypeClass(col)),
                ClassName.get(getColumnTypeClass(col))), "processor");
        String args = getArgumentsFromRowComponents(tableMetadata);
        updateColumnIfExistsBuilder
                .addStatement("$T<$T> result = get$L($L)",
                        Optional.class, getColumnTypeClass(col), VarName(col), args)
                .beginControlFlow("if (result.isPresent())")
                .addStatement("$T newValue = processor.apply(result.get())", getColumnTypeClass(col))
                .beginControlFlow("if (newValue != result.get())")
                .addStatement("put$L($L, processor.apply(result.get()))", VarName(col), args)
                .endControlFlow()
                .endControlFlow();
        return updateColumnIfExistsBuilder.build();
    }
}