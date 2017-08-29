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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.stream.Collectors;

import javax.annotation.Generated;
import javax.lang.model.element.Modifier;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.generation.Triggers;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.WildcardTypeName;

public final class TableFactoryRenderer {
    private final String schemaName;
    private final String packageName;
    private final String defaultNamespace;
    private final SortedMap<String, TableDefinition> definitions;
    private final ClassName tableFactoryType;
    private final ClassName sharedTriggersType;

    private TableFactoryRenderer(String schemaName,
            String packageName,
            Namespace defaultNamespace,
            Map<String, TableDefinition> definitions) {
        this.schemaName = schemaName;
        this.packageName = packageName;
        this.definitions = Maps.newTreeMap();
        this.defaultNamespace = defaultNamespace.getName();
        this.tableFactoryType = ClassName.get(packageName, getClassName());
        this.sharedTriggersType = tableFactoryType.nestedClass("SharedTriggers");
        for (Entry<String, TableDefinition> entry : definitions.entrySet()) {
            this.definitions.put(Renderers.getClassTableName(entry.getKey(), entry.getValue()), entry.getValue());
        }
    }

    public static TableFactoryRenderer of(String schemaName,
            String packageName,
            Namespace defaultNamespace,
            Map<String, TableDefinition> definitions) {
        return new TableFactoryRenderer(schemaName, packageName, defaultNamespace, definitions);

    }

    public String getPackageName() {
        return packageName;
    }

    public String getClassName() {
        return schemaName + "TableFactory";
    }

    public String render() {
        TypeSpec.Builder tableFactory = TypeSpec.classBuilder(getClassName())
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addAnnotation(AnnotationSpec.builder(Generated.class)
                        .addMember("value", "$S", TableFactoryRenderer.class.getName())
                        .build());

        for (FieldSpec field : getFields()) {
            tableFactory.addField(field);
        }
        for (TypeSpec subType : getSubTypes()) {
            tableFactory.addType(subType);
        }
        for (MethodSpec method : getConstructors()) {
            tableFactory.addMethod(method);
        }
        for (MethodSpec method : getMethods()) {
            tableFactory.addMethod(method);
        }

        JavaFile javaFile = JavaFile.builder(packageName, tableFactory.build())
                .indent("    ")
                .build();

        return javaFile.toString();
    }

    private String getTableName(String name) {
        return name + "Table";
    }

    private List<FieldSpec> getFields() {
        ArrayList<FieldSpec> results = new ArrayList<>();

        TypeName functionOfTransactionAndTriggersType = ParameterizedTypeName.get(
                ClassName.get(Function.class),
                WildcardTypeName.supertypeOf(Transaction.class), sharedTriggersType);

        results.add(getDefaultNamespaceField());
        results.add(FieldSpec.builder(ParameterizedTypeName.get(
                ClassName.get(List.class), functionOfTransactionAndTriggersType), "sharedTriggers")
                .addModifiers(Modifier.PRIVATE, Modifier.FINAL)
                .build());
        results.add(FieldSpec.builder(Namespace.class, "namespace")
                .addModifiers(Modifier.PRIVATE, Modifier.FINAL)
                .build());

        return results;
    }

    private List<TypeSpec> getSubTypes() {
        ArrayList<TypeSpec> results = new ArrayList<>();
        results.add(getSharedTriggers());
        results.add(getNullSharedTriggers(sharedTriggersType));

        return results;
    }

    private List<MethodSpec> getConstructors() {
        ArrayList<MethodSpec> results = new ArrayList<>();

        TypeName functionOfTransactionAndTriggersType = ParameterizedTypeName.get(
                ClassName.get(Function.class),
                WildcardTypeName.supertypeOf(Transaction.class), sharedTriggersType);
        TypeName sharedTriggersListType = ParameterizedTypeName.get(
                ClassName.get(List.class), functionOfTransactionAndTriggersType);

        results.add(factoryBaseBuilder()
                .addParameter(ParameterizedTypeName.get(
                        ClassName.get(List.class), functionOfTransactionAndTriggersType), "sharedTriggers")
                .addParameter(Namespace.class, "namespace")
                .addStatement("return new $T($N, $N)", tableFactoryType, "sharedTriggers", "namespace")
                .build());

        results.add(factoryBaseBuilder()
                .addParameter(sharedTriggersListType, "sharedTriggers")
                .addStatement("return new $T($N, $N)", tableFactoryType, "sharedTriggers", "defaultNamespace")
                .build());

        results.add(factoryBaseBuilder()
                .addParameter(Namespace.class, "namespace")
                .addStatement("return of($T.<$T>of(), $N)",
                        ImmutableList.class,
                        functionOfTransactionAndTriggersType,
                        "namespace")
                .build());

        results.add(factoryBaseBuilder()
                .addStatement("return of($T.<$T>of(), $N)",
                        ImmutableList.class,
                        functionOfTransactionAndTriggersType,
                        "defaultNamespace")
                .build());

        results.add(MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PRIVATE)
                .addParameter(sharedTriggersListType, "sharedTriggers")
                .addParameter(Namespace.class, "namespace")
                .addStatement("this.$N = $N", "sharedTriggers", "sharedTriggers")
                .addStatement("this.$N = $N", "namespace", "namespace")
                .build());

        return results;
    }

    private MethodSpec.Builder factoryBaseBuilder() {
        return MethodSpec.methodBuilder("of")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                .returns(tableFactoryType);
    }

    private List<MethodSpec> getMethods() {
        return definitions.entrySet()
                .stream()
                .map(entry -> getTableMethod(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    private MethodSpec getTableMethod(String name, TableDefinition tableDefinition) {
        String tableName = getTableName(name);
        String triggerName = tableName + "." + name + "Trigger";
        TypeName tableType = ClassName.get(packageName, tableName);
        TypeName triggerType = ClassName.get(packageName, triggerName);
        MethodSpec.Builder tableGetterMethodBuilder = MethodSpec.methodBuilder("get" + tableName)
                .addModifiers(Modifier.PUBLIC)
                .addParameter(Transaction.class, "t")
                .returns(tableType);
        if (tableDefinition.getGenericTableName() != null) {
            tableGetterMethodBuilder
                    .addParameter(String.class, "name")
                    .addParameter(ArrayTypeName.of(triggerType), "triggers")
                    .varargs()
                    .addStatement("return $T.of(t, namespace, name, $T.getAllTriggers(t, sharedTriggers, triggers))",
                            tableType,
                            Triggers.class);
        } else {
            tableGetterMethodBuilder
                    .addParameter(ArrayTypeName.of(triggerType), "triggers")
                    .varargs()
                    .addStatement("return $T.of(t, namespace, $T.getAllTriggers(t, sharedTriggers, triggers))",
                            tableType,
                            Triggers.class);
        }
        return tableGetterMethodBuilder.build();
    }

    private TypeSpec getSharedTriggers() {
        TypeSpec.Builder sharedTriggersInterfaceBuilder = TypeSpec.interfaceBuilder("SharedTriggers")
                .addModifiers(Modifier.PUBLIC);

        for (String name : definitions.keySet()) {
            String tableName = getTableName(name);
            String triggerName = tableName + "." + name + "Trigger";
            TypeName triggerType = ClassName.get(packageName, triggerName);
            sharedTriggersInterfaceBuilder.addSuperinterface(triggerType);
        }

        return sharedTriggersInterfaceBuilder.build();
    }

    private TypeSpec getNullSharedTriggers(TypeName sharedTriggersInterfaceType) {
        TypeSpec.Builder nullSharedTriggersClassBuilder = TypeSpec.classBuilder("NullSharedTriggers")
                .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT, Modifier.STATIC)
                .addSuperinterface(sharedTriggersInterfaceType);

        for (Entry<String, TableDefinition> entry : definitions.entrySet()) {
            String name = entry.getKey();
            TableDefinition tableDefinition = entry.getValue();
            String tableName = getTableName(name);
            TypeName rowType = ClassName.get(packageName, tableName + "." + name + "Row");
            TypeName columnType = ClassName.get(packageName, tableName + "." + name + "ColumnValue");
            if (!tableDefinition.toTableMetadata().getColumns().hasDynamicColumns()) {
                columnType = ParameterizedTypeName.get(
                        ClassName.get(packageName, tableName + "." + name + "NamedColumnValue"),
                        WildcardTypeName.subtypeOf(Object.class));
            }
            MethodSpec putMethod = MethodSpec.methodBuilder("put" + name)
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .addParameter(
                            ParameterizedTypeName.get(
                                    ClassName.get(Multimap.class),
                                    rowType,
                                    WildcardTypeName.subtypeOf(columnType)
                            ), "newRows")
                    .addComment("do nothing")
                    .build();
            nullSharedTriggersClassBuilder
                    .addMethod(putMethod);
        }

        return nullSharedTriggersClassBuilder.build();
    }

    private FieldSpec getDefaultNamespaceField() {
        FieldSpec.Builder namespaceFieldBuilder = FieldSpec.builder(Namespace.class, "defaultNamespace")
                .addModifiers(Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC);

        if (defaultNamespace.isEmpty()) {
            namespaceFieldBuilder.initializer("$T.create($S, $T.EMPTY_NAMESPACE)",
                    Namespace.class, "default", Namespace.class);
        } else {
            namespaceFieldBuilder.initializer("$T.create($S, $T.UNCHECKED_NAME)",
                    Namespace.class, defaultNamespace, Namespace.class);
        }
        return namespaceFieldBuilder.build();
    }
}
