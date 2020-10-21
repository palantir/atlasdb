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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class ForeignKeyConstraintMetadata {

    private final boolean isThisGeneric;
    private final String thisTableName;
    private final boolean isOtherGeneric;
    private final String otherGenericTable;
    private final String otherTableName;
    private Class<? extends ForeignKeyConstraint> constraintClass;

    private final List<String> rowVariables;
    private final List<String> columnVariables;
    private final List<String> allVariables;

    public String getThisTableName() {
        return thisTableName;
    }

    public boolean isThisGeneric() {
        return isThisGeneric;
    }

    public static Builder builder(String tableName, Class<? extends ForeignKeyConstraint> clazz) {
        return new Builder(false, null, false, null, tableName, clazz);
    }

    public static Builder builderOtherGeneric(
            String otherGenericTable, String otherTableName, Class<? extends ForeignKeyConstraint> clazz) {
        return new Builder(false, null, true, otherGenericTable, otherTableName, clazz);
    }

    public static Builder builderThisGeneric(
            String thisTableName, String otherTableName, Class<? extends ForeignKeyConstraint> clazz) {
        return new Builder(true, thisTableName, false, null, otherTableName, clazz);
    }

    public static Builder builderThisAndOtherGeneric(
            String thisTableName,
            String otherGenericTable,
            String otherTableName,
            Class<? extends ForeignKeyConstraint> clazz) {
        return new Builder(true, thisTableName, true, otherGenericTable, otherTableName, clazz);
    }

    private ForeignKeyConstraintMetadata(
            String otherTableName,
            Class<? extends ForeignKeyConstraint> clazz,
            List<String> rowVariables,
            List<String> columnVariables,
            List<String> allVariables,
            String otherGenericTable,
            boolean isOtherGeneric,
            String thisTableName,
            boolean isThisGeneric) {
        this.otherTableName = otherTableName;
        this.rowVariables = ImmutableList.copyOf(rowVariables);
        this.columnVariables = ImmutableList.copyOf(columnVariables);
        this.allVariables = ImmutableList.copyOf(allVariables);
        this.constraintClass = clazz;
        this.otherGenericTable = otherGenericTable;
        this.isOtherGeneric = isOtherGeneric;
        this.thisTableName = thisTableName;
        this.isThisGeneric = isThisGeneric;
    }

    public String getOtherTableName() {
        return otherTableName;
    }

    public Class<? extends ForeignKeyConstraint> getConstraintClass() {
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

    public String getOtherGenericTable() {
        return otherGenericTable;
    }

    public boolean isOtherGeneric() {
        return isOtherGeneric;
    }

    public static final class Builder {
        private final boolean isThisGeneric;
        private final String thisTableName;
        private final boolean isOtherGeneric;
        private final String otherGenericTable;
        private final String otherTableName;
        private final Class<? extends ForeignKeyConstraint> constraintClass;

        private final List<String> rowVariables = new ArrayList<>();
        private final List<String> columnVariables = new ArrayList<>();
        private final List<String> allVariables = new ArrayList<>();

        public Builder(
                boolean isThisGeneric,
                String thisTableName,
                boolean isOtherGeneric,
                String otherGenericTable,
                String otherTableName,
                Class<? extends ForeignKeyConstraint> constraintClass) {
            this.isThisGeneric = isThisGeneric;
            this.thisTableName = thisTableName;
            this.isOtherGeneric = isOtherGeneric;
            this.otherGenericTable = otherGenericTable;
            this.otherTableName = otherTableName;
            this.constraintClass = constraintClass;
        }

        public Builder addRowVariables(String... variables) {
            Collections.addAll(rowVariables, variables);
            Collections.addAll(allVariables, variables);
            return this;
        }

        public Builder addColumnVariables(String... variables) {
            Collections.addAll(columnVariables, variables);
            Collections.addAll(allVariables, variables);
            return this;
        }

        public ForeignKeyConstraintMetadata build() {
            return new ForeignKeyConstraintMetadata(
                    otherTableName,
                    constraintClass,
                    rowVariables,
                    columnVariables,
                    allVariables,
                    otherGenericTable,
                    isOtherGeneric,
                    thisTableName,
                    isThisGeneric);
        }
    }
}
