/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
import static com.palantir.atlasdb.table.description.render.ColumnRenderers.short_name;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.function.Function;

import javax.lang.model.element.Modifier;

import com.google.common.base.MoreObjects;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.table.api.TypedRowResult;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;

class NamedRowResultRenderer extends Renderer {
    private final String packageName;

    private final String tableName;
    private final ClassName tableType;

    private final String row;
    private final ClassName rowType;

    private final String rowResult;
    private final ClassName rowResultType;

    private final SortedSet<NamedColumnDescription> cols;

    public NamedRowResultRenderer(Renderer parent, String packageName, String tableName, SortedSet<NamedColumnDescription> cols) {
        super(parent);
        this.packageName = packageName;
        this.tableName = tableName;
        this.tableType = ClassName.get(packageName, tableName + "Table");
        this.row = tableName + "Row";
        this.rowType = tableType.nestedClass(this.row);
        this.rowResult = tableName + "RowResult";
        this.rowResultType = tableType.nestedClass(this.rowResult);
        this.cols = cols;
    }

    @Override
    protected void run() {
        TypeSpec.Builder rowResultClassBuilder = TypeSpec.classBuilder(this.rowResult)
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addSuperinterface(TypedRowResult.class);

        for (FieldSpec field : getFields()) {
            rowResultClassBuilder.addField(field);
        }
        for (MethodSpec staticFactory : getStaticFactories()) {
            rowResultClassBuilder.addMethod(staticFactory);
        }
        for (MethodSpec constructor : getConstructors()) {
            rowResultClassBuilder.addMethod(constructor);
        }
        for (MethodSpec method : getMethods()) {
            rowResultClassBuilder.addMethod(method);
        }

        addBlock(rowResultClassBuilder.build().toString());
    }

    private List<FieldSpec> getFields() {
        ArrayList<FieldSpec> results = new ArrayList<>();
        results.add(FieldSpec.builder(
                ParameterizedTypeName.get(
                    ClassName.get(RowResult.class),
                    ArrayTypeName.of(byte.class)),
                "row")
                .addModifiers(Modifier.PRIVATE, Modifier.FINAL)
                .build());
        return results;
    }

    private List<MethodSpec> getStaticFactories() {
        ArrayList<MethodSpec> results = new ArrayList<>();
        results.add(MethodSpec.methodBuilder("of")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                .addParameter(ParameterizedTypeName.get(
                        ClassName.get(RowResult.class),
                        ArrayTypeName.of(byte.class)),
                "row")
                .returns(rowResultType)
                .addStatement("return new $T(row)", rowResultType)
                .build());
        return results;
    }

    private List<MethodSpec> getConstructors() {
        ArrayList<MethodSpec> results = new ArrayList<>();
        results.add(MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PRIVATE)
                .addParameter(ParameterizedTypeName.get(
                        ClassName.get(RowResult.class),
                        ArrayTypeName.of(byte.class)),
                        "row")
                .addStatement("this.$N = $N", "row", "row")
                .build());
        return results;
    }

    private List<MethodSpec> getMethods() {
        ArrayList<MethodSpec> results = new ArrayList<>();
        results.add(getRowName());
        results.add(getRowNameFun());
        results.add(fromRawRowResultFun());
        for (NamedColumnDescription col : cols) {
            results.add(hasCol(col));
        }
        for (NamedColumnDescription col : cols) {
            results.add(getCol(col));
        }
        for (NamedColumnDescription col : cols) {
            results.add(getColFun(col));
        }
        results.add(renderToString());
        return results;
    }


    private MethodSpec getRowName() {
        return MethodSpec.methodBuilder("getRowName")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(rowType)
                .addStatement("return $T.BYTES_HYDRATOR.hydrateFromBytes(row.getRowName())", rowType)
                .build();
    }

    private MethodSpec getRowNameFun() {
        return MethodSpec.methodBuilder("getRowNameFun")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                .returns(ParameterizedTypeName.get(
                        ClassName.get(Function.class),
                        rowResultType,
                        rowType))
                .addStatement("return (rowResult) -> { return rowResult.getRowName(); }")
                .build();
    }

    private MethodSpec fromRawRowResultFun() {
        return MethodSpec.methodBuilder("fromRawRowResultFun")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                .returns(ParameterizedTypeName.get(
                        ClassName.get(Function.class),
                        ParameterizedTypeName.get(
                                ClassName.get(RowResult.class),
                                ArrayTypeName.of(byte.class)
                        ),
                        rowResultType))
                .addStatement("return (rowResult) -> { return new $T(rowResult); }", rowResultType)
                .build();
    }

    private MethodSpec hasCol(NamedColumnDescription col) {
        return MethodSpec.methodBuilder("has" + VarName(col))
                .addModifiers(Modifier.PUBLIC)
                .returns(boolean.class)
                .addStatement("return row.getColumns().containsKey(PtBytes.toCachedBytes($N))", short_name(col))
                .build();
    }

    private MethodSpec getCol(NamedColumnDescription col) {
        return MethodSpec.methodBuilder("get" + VarName(col))
                .addModifiers(Modifier.PUBLIC)
                .returns(col.getValue().getValueType().getTypeClass())
                .addStatement("byte[] bytes = row.getColumns().get(PtBytes.toCachedBytes($N))",
                        short_name(col))
                .beginControlFlow("if (bytes == null)")
                    .addStatement("return null")
                .endControlFlow()
                .addStatement("$T value = $T.BYTES_HYDRATOR.hydrateFromBytes(bytes)",
                        this.tableType.nestedClass(Renderers.CamelCase(col.getLongName())),
                        this.tableType.nestedClass(Renderers.CamelCase(col.getLongName())))
                .addStatement("return value.getValue()")
                .build();
    }

    private MethodSpec getColFun(NamedColumnDescription col) {
        return MethodSpec.methodBuilder("get" + VarName(col) + "Fun")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                .returns(ParameterizedTypeName.get(
                        ClassName.get(Function.class),
                        rowResultType,
                        ClassName.get(col.getValue().getValueType().getTypeClass())))
                .addStatement("return (rowResult) -> { return rowResult.get$N(); }", VarName(col))
                .build();
    }

    private MethodSpec renderToString() {
        CodeBlock.Builder stringRendererCode = CodeBlock.builder();
        stringRendererCode.add("return $T.toStringHelper(getClass().getSimpleName())\n", MoreObjects.class);
        stringRendererCode.add("    .add($S, getRowName())\n", "RowName");
        for (NamedColumnDescription col : cols) {
            stringRendererCode.add("    .add($S, get$N())\n", VarName(col), VarName(col));
        }
        stringRendererCode.add("    .toString();");

        return MethodSpec.methodBuilder("toString")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(String.class)
                .addCode(stringRendererCode.build())
                .build();
    }
}
