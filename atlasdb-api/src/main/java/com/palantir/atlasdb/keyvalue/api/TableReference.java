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
package com.palantir.atlasdb.keyvalue.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

public final class TableReference {
    private final Namespace namespace;
    private final String tableName;

    /**
     * Creates a table reference based on fullTableName.
     * fullTableName is assumed to be of the format namespace.tableName, and must contain a dot.
     */
    public static TableReference createFromFullyQualifiedName(String fullTableName) {
        int index = fullTableName.indexOf('.');
        Preconditions.checkArgument(index > 0, "Table name %s is not a fully qualified table name.", fullTableName);
        return create(
                Namespace.create(fullTableName.substring(0, index), Namespace.UNCHECKED_NAME),
                fullTableName.substring(index + 1));
    }

    public static TableReference create(Namespace namespace, String tableName) {
        return new TableReference(namespace, tableName);
    }

    /**
     * Creates a table reference with an empty namespace, based on tablename.
     * This should only be used when creating a TableReference for a system table.
     */
    public static TableReference createWithEmptyNamespace(String tableName) {
        return new TableReference(Namespace.EMPTY_NAMESPACE, tableName);
    }

    public static TableReference createLowerCased(TableReference table) {
        String name = table.namespace.getName().toLowerCase();
        Namespace namespace = name.isEmpty() ? Namespace.EMPTY_NAMESPACE : Namespace.create(name);
        return create(namespace, table.tableName.toLowerCase());
    }

    /**
     * @deprecated please use createFromFullyQualifiedName, if fullTableName includes the namespace,
     * or createWithEmptyNamespace, if you're passing in a system table name.
     */
    @Deprecated
    @SuppressWarnings("InlineMeSuggester")
    public static TableReference createUnsafe(String fullTableName) {
        return fullTableName.indexOf('.') < 0
                ? createWithEmptyNamespace(fullTableName)
                : createFromFullyQualifiedName(fullTableName);
    }

    @JsonCreator
    private TableReference(
            @JsonProperty("namespace") Namespace namespace, @JsonProperty("tablename") String tableName) {
        this.namespace = namespace;
        this.tableName = tableName;
    }

    /**
     * @deprecated uses {@link TableReference#createUnsafe}, which is itself deprecated.
     */
    @Deprecated
    public static TableReference fromInternalTableName(String tableName) {
        if (tableName.startsWith("_")) {
            return createWithEmptyNamespace(tableName);
        }
        return createUnsafe(tableName.replaceFirst("__", "."));
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public String getTablename() {
        return getTableName();
    }

    public String getTableName() {
        return tableName;
    }

    @JsonIgnore
    public String getQualifiedName() {
        return namespace.isEmptyNamespace() || namespace.getName().equals("met")
                ? tableName
                : namespace.getName() + "." + tableName;
    }

    public static boolean isFullyQualifiedName(String tableName) {
        return tableName.contains(".");
    }

    @JsonIgnore
    public boolean isFullyQualifiedName() {
        return getQualifiedName().contains(".");
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TableReference that = (TableReference) obj;
        return Objects.equals(namespace, that.namespace) && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, tableName);
    }

    @Override
    public String toString() {
        return getQualifiedName();
    }

    public static TableReference fromString(String tableReferenceAsString) {
        int dotCount = StringUtils.countMatches(tableReferenceAsString, ".");
        if (dotCount == 0) {
            return TableReference.createWithEmptyNamespace(tableReferenceAsString);
        } else if (dotCount == 1) {
            return TableReference.createFromFullyQualifiedName(tableReferenceAsString);
        } else {
            throw new IllegalArgumentException(tableReferenceAsString + " is not a valid table reference.");
        }
    }
}
