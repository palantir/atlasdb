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

import static com.palantir.atlasdb.table.description.render.ColumnRenderers.TypeName;
import static com.palantir.atlasdb.table.description.render.ColumnRenderers.long_name;
import static com.palantir.atlasdb.table.description.render.ColumnRenderers.short_name;

import java.util.ArrayList;
import java.util.List;

import javax.lang.model.element.Modifier;

import com.google.common.base.MoreObjects;
import com.palantir.atlasdb.compress.CompressionUtils;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.common.persist.Persistable;
import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;

public class NamedColumnValueRenderer extends Renderer {
    private final String packageName;
    private final String tableName;
    private final ClassName tableType;
    private final String columnName;
    private final ClassName columnType;
    private final NamedColumnDescription col;
    private final ClassName javaType;

    public NamedColumnValueRenderer(Renderer parent, String packageName, String tableName, NamedColumnDescription col) {
        super(parent);
        this.packageName = packageName;

        this.tableName = tableName;
        this.tableType = ClassName.get(packageName, tableName + "Table");

        this.columnName = Renderers.CamelCase(col.getLongName());
        this.columnType = tableType.nestedClass(this.columnName);

        this.col = col;
        this.javaType = ClassName.get(col.getValue().getValueType().getTypeClass());
    }

    @Override
    protected void run() {
        TypeSpec.Builder columnClassBuilder = TypeSpec.classBuilder(this.columnName)
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addSuperinterface(ParameterizedTypeName.get(
                        tableType.nestedClass(tableName + "NamedColumnValue"),
                        javaType
                ))
                .addJavadoc(getJavaDoc());

        for (FieldSpec field : getFields()) {
            columnClassBuilder.addField(field);
        }
        for (MethodSpec staticFactory : getStaticFactories()) {
            columnClassBuilder.addMethod(staticFactory);
        }
        for (MethodSpec constructor : getConstructors()) {
            columnClassBuilder.addMethod(constructor);
        }
        for (MethodSpec method : getMethods()) {
            columnClassBuilder.addMethod(method);
        }

        addBlock(columnClassBuilder.build().toString());
    }

    private String getJavaDoc() {
        String javaDoc = "<pre> \n"
                + "Column value description {\n"
                + " type: " + TypeName(col) + ";\n";
        if (col.getValue().getProtoDescriptor() != null) {
            String protoDescription = col.getValue().getProtoDescriptor().toProto().toString();
            for (String line : protoDescription.split("\n")) {
                javaDoc += "   " + line + "\n";
            }
        }
        javaDoc += "}\n"
                + "</pre>";

        return javaDoc;
    }

    private List<FieldSpec> getFields() {
        ArrayList<FieldSpec> results = new ArrayList<>();
        results.add(FieldSpec.builder(javaType, "value")
                .addModifiers(Modifier.FINAL)
                .build());
        results.add(bytesHydrator());
        return results;
    }

    private List<MethodSpec> getStaticFactories() {
        ArrayList<MethodSpec> results = new ArrayList<>();
        results.add(MethodSpec.methodBuilder("of")
                .addModifiers(Modifier.STATIC, Modifier.PUBLIC)
                .addParameter(javaType, "value")
                .returns(columnType)
                .addStatement("return new $T(value)", columnType)
                .build());
        return results;
    }

    private List<MethodSpec> getConstructors() {
        ArrayList<MethodSpec> results = new ArrayList<>();
        results.add(MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PRIVATE)
                .addParameter(javaType, "value")
                .addStatement("this.$N = $N", "value", "value")
                .build());
        return results;
    }

    private List<MethodSpec> getMethods() {
        ArrayList<MethodSpec> results = new ArrayList<>();
        results.add(getColumnName());
        results.add(getShortColumnName());
        results.add(getValue());
        results.add(persistValue());
        results.add(persistColumnName());
        results.add(renderToString());
        return results;
    }

    private MethodSpec getColumnName() {
        return MethodSpec.methodBuilder("getColumnName")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(String.class)
                .addStatement("return $N", long_name(col))
                .build();
    }

    private MethodSpec getShortColumnName() {
        return MethodSpec.methodBuilder("getShortColumnName")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(String.class)
                .addStatement("return $N", short_name(col))
                .build();
    }

    private MethodSpec getValue() {
        return MethodSpec.methodBuilder("getValue")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(javaType)
                .addStatement("return value")
                .build();
    }

    private MethodSpec persistValue() {
        String persistingToBytesCode;
        switch (col.getValue().getFormat()) {
            case PERSISTABLE:
                persistingToBytesCode = "value.persistToBytes()";
                break;
            case PROTO:
                persistingToBytesCode = "value.toByteArray()";
                break;
            case PERSISTER:
                persistingToBytesCode = col.getValue().getPersistCode("value");
                break;
            case VALUE_TYPE:
                persistingToBytesCode = col.getValue().getValueType().getPersistCode("value");
                break;
            default:
                throw new UnsupportedOperationException("Unsupported value type: " + col.getValue().getFormat());
        }

        return MethodSpec.methodBuilder("persistValue")
                 .addAnnotation(Override.class)
                 .addModifiers(Modifier.PUBLIC)
                 .returns(ArrayTypeName.of(byte.class))
                 .addStatement("byte[] bytes = $N", persistingToBytesCode)
                 .addStatement("return $T.compress(bytes, $T.$N)",
                         CompressionUtils.class,
                         ColumnValueDescription.Compression.class,
                         col.getValue().getCompression().name())
                 .build();
    }

    private MethodSpec persistColumnName() {
        return MethodSpec.methodBuilder("persistColumnName")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(ArrayTypeName.of(byte.class))
                .addStatement("return $T.toCachedBytes($N)", PtBytes.class, short_name(col))
                .build();
    }

    private FieldSpec bytesHydrator() {
        CodeBlock.Builder parsingFromBytes = CodeBlock.builder();
        switch (col.getValue().getFormat()) {
            case PERSISTABLE:
                parsingFromBytes.add("return of(", TypeName(col), ".BYTES_HYDRATOR.hydrateFromBytes(bytes));");
                break;
            case PROTO:
                parsingFromBytes.add("try {");
                parsingFromBytes.add("  return of($T.parseFrom(bytes));", javaType);
                parsingFromBytes.add("} catch (InvalidProtocolBufferException e) {");
                parsingFromBytes.add("  throw Throwables.throwUncheckedException(e);");
                parsingFromBytes.add("}");
                break;
            case PERSISTER:
                parsingFromBytes.add("return of($N);", col.getValue().getHydrateCode("bytes"));
                break;
            case VALUE_TYPE:
                parsingFromBytes.add("return of($N);",
                        col.getValue().getValueType().getHydrateCode("bytes", "0"));
                break;
            default:
                throw new UnsupportedOperationException("Unsupported value type: " + col.getValue().getFormat());
        }

        TypeSpec hydratorType = TypeSpec.anonymousClassBuilder("")
                .addSuperinterface(ParameterizedTypeName.get(ClassName.get(Persistable.Hydrator.class), columnType))
                .addMethod(MethodSpec.methodBuilder("hydrateFromBytes")
                        .addAnnotation(Override.class)
                        .addModifiers(Modifier.PUBLIC)
                        .addParameter(ArrayTypeName.of(byte.class), "bytes")
                        .returns(columnType)
                        .addStatement("bytes = $T.decompress(bytes, $T.$N)",
                                CompressionUtils.class,
                                ColumnValueDescription.Compression.class,
                                col.getValue().getCompression().name()
                        )
                        .addCode(parsingFromBytes.build())
                        .build())
                .build();

        return FieldSpec.builder(
                ParameterizedTypeName.get(
                        ClassName.get(Persistable.Hydrator.class),
                        columnType),
                "BYTES_HYDRATOR")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .initializer("$L", hydratorType)
                .build();
    }

    private MethodSpec renderToString() {
        return MethodSpec.methodBuilder("toString")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(String.class)
                .addStatement("return $T.toStringHelper(getClass().getSimpleName())"
                        + ".add(\"Value\", this.value).toString()", MoreObjects.class)
                .build();
    }
}
