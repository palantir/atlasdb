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
package com.palantir.atlasdb.table.description.constraints;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;

public class RowConstraintMetadata {
    private final Class<? extends RowConstraint> constraintClass;
    private final boolean isGeneric;
    private final String tableName;
    private final List<String> rowVariables;
    private final List<String> columnVariables;
    private final List<String> allVariables;

    public static Builder builder(Class<? extends RowConstraint> clazz) {
        return new Builder(clazz, false, null);
    }

    public static Builder builderGeneric(Class<? extends RowConstraint> clazz, String tableName) {
        return new Builder(clazz, true, tableName);
    }
    private RowConstraintMetadata(Class<? extends RowConstraint> constraintClass,
                                  List<String> rowVariables,
                                  List<String> columnVariables,
                                  List<String> allVariables,
                                  boolean isGeneric,
                                  String tableName) {
        this.constraintClass = constraintClass;
        this.rowVariables = ImmutableList.copyOf(rowVariables);
        this.columnVariables = ImmutableList.copyOf(columnVariables);
        this.allVariables = ImmutableList.copyOf(allVariables);
        this.isGeneric = isGeneric;
        this.tableName = tableName;
    }

    public boolean isGeneric() {
        return isGeneric;
    }

    public String getTableName() {
        return tableName;
    }

    public Class<? extends RowConstraint> getConstraintClass() {
        return constraintClass;
    }

    public List<String> getRowVariables() {
        return rowVariables;
    }

    public List<String> getColumnVariables() {
        return columnVariables;
    }

    public List<String> getAllVariables() {
        return allVariables;
    }

    public static final class Builder {
        private final Class<? extends RowConstraint> constraintClass;
        private final boolean isGeneric;
        private final String tableName;
        private final List<String> rowVariables = Lists.newArrayList();
        private final List<String> columnVariables = Lists.newArrayList();
        private final List<String> allVariables = Lists.newArrayList();

        public Builder(Class<? extends RowConstraint> constraintClass, boolean isGeneric, String tableName) {
            this.constraintClass = constraintClass;
            this.isGeneric = isGeneric;
            this.tableName = tableName;
        }

        public Builder addRowVariables(String ... variables) {
            Collections.addAll(rowVariables, variables);
            Collections.addAll(allVariables, variables);
            return this;
        }

        public Builder addColumnVariables(String ... variables) {
            Collections.addAll(columnVariables, variables);
            Collections.addAll(allVariables, variables);
            return this;
        }

        public RowConstraintMetadata build() {
            return new RowConstraintMetadata(constraintClass, rowVariables, columnVariables,
                    allVariables, isGeneric, tableName);
        }
    }
}
