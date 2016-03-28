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
package com.palantir.atlasdb.keyvalue.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;


public class TableReference {
    private final Namespace namespace;
    private final String tablename;

    public static TableReference createFromFullyQualifiedName(String fullTableName) {
        int index = fullTableName.indexOf('.');
        Preconditions.checkArgument(index > 0, "Table name %s is not a fully qualified table name.");
        return create(Namespace.create(fullTableName.substring(0, index), Namespace.UNCHECKED_NAME), fullTableName.substring(index + 1));
    }

    public static TableReference create(Namespace namespace, String tablename) {
        return new TableReference(namespace, tablename);
    }

    public static TableReference createWithEmptyNamespace(String tablename) {
        return new TableReference(Namespace.EMPTY_NAMESPACE, tablename);
    }

    public static TableReference createUnsafe(String fullTableName) {
        return fullTableName.indexOf('.') < 0 ? createWithEmptyNamespace(fullTableName) : createFromFullyQualifiedName(fullTableName);
    }

    public static boolean isFullyQualifiedName(String tableName) {
        return tableName.contains(".");
    }

    @JsonCreator
    private TableReference(@JsonProperty("namespace") Namespace namespace, @JsonProperty("tablename") String tablename) {
        this.namespace = namespace;
        this.tablename = tablename;
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public String getTablename() {
        return tablename;
    }

    @JsonIgnore
    public String getQualifiedName() {
        return namespace.isEmptyNamespace() || namespace.getName().equals("met") ? tablename : namespace.getName() + "." + tablename;
    }

    @JsonIgnore
    public boolean isFullyQualifiedName() {
        return getQualifiedName().contains(".");
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((namespace == null) ? 0 : namespace.hashCode());
        result = prime * result + ((tablename == null) ? 0 : tablename.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TableReference other = (TableReference) obj;
        if (namespace == null) {
            if (other.namespace != null)
                return false;
        } else if (!namespace.equals(other.namespace))
            return false;
        if (tablename == null) {
            if (other.tablename != null)
                return false;
        } else if (!tablename.equals(other.tablename))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "TableReference [namespace=" + namespace + ", tablename=" + tablename + "]";
    }
}
