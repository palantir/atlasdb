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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class ConstraintMetadata {
    private final List<RowConstraintMetadata> rowConstraints;
    private final List<TableConstraint> tableConstraints;
    private final List<ForeignKeyConstraintMetadata> foreignKeyConstraints;

    public static Builder builder() {
        return new Builder();
    }

    public static ConstraintMetadata none() {
        return new ConstraintMetadata(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    }

    private ConstraintMetadata(
            List<RowConstraintMetadata> rowConstraints,
            List<TableConstraint> tableConstraints,
            List<ForeignKeyConstraintMetadata> foreignKeyConstraints) {
        this.rowConstraints = ImmutableList.copyOf(rowConstraints);
        this.tableConstraints = ImmutableList.copyOf(tableConstraints);
        this.foreignKeyConstraints = ImmutableList.copyOf(foreignKeyConstraints);
    }

    public List<RowConstraintMetadata> getRowConstraints() {
        return rowConstraints;
    }

    public List<TableConstraint> getTableConstraints() {
        return tableConstraints;
    }

    public List<ForeignKeyConstraintMetadata> getForeignKeyConstraints() {
        return foreignKeyConstraints;
    }

    public boolean isEmpty() {
        return rowConstraints.isEmpty() && tableConstraints.isEmpty() && foreignKeyConstraints.isEmpty();
    }

    public Set<String> getAllRowVariableNames() {
        Set<String> names = new HashSet<>();
        for (RowConstraintMetadata constraint : rowConstraints) {
            names.addAll(constraint.getRowVariables());
        }
        for (ForeignKeyConstraintMetadata constraint : foreignKeyConstraints) {
            names.addAll(constraint.getRowVariables());
        }
        return names;
    }

    public Set<String> getAllColumnVariableNames() {
        Set<String> names = new HashSet<>();
        for (RowConstraintMetadata constraint : rowConstraints) {
            names.addAll(constraint.getColumnVariables());
        }
        for (ForeignKeyConstraintMetadata constraint : foreignKeyConstraints) {
            names.addAll(constraint.getColumnVariables());
        }
        return names;
    }

    public static final class Builder {
        private final List<RowConstraintMetadata> rowConstraints = new ArrayList<>();
        private final List<TableConstraint> tableConstraints = new ArrayList<>();
        private final List<ForeignKeyConstraintMetadata> foreignKeyConstraints = new ArrayList<>();

        Builder() {
            /**/
        }

        public Builder addRowConstraint(RowConstraintMetadata constraint) {
            rowConstraints.add(constraint);
            return this;
        }

        public Builder addRowConstraints(List<RowConstraintMetadata> constraints) {
            rowConstraints.addAll(constraints);
            return this;
        }

        public Builder addTableConstraint(TableConstraint constraint) {
            tableConstraints.add(constraint);
            return this;
        }

        public Builder addTableConstraints(List<TableConstraint> constraints) {
            tableConstraints.addAll(constraints);
            return this;
        }

        public Builder addForeignKeyConstraint(ForeignKeyConstraintMetadata constraint) {
            foreignKeyConstraints.add(constraint);
            return this;
        }

        public Builder addForeignKeyConstraints(List<ForeignKeyConstraintMetadata> constraints) {
            foreignKeyConstraints.addAll(constraints);
            return this;
        }

        public ConstraintMetadata build() {
            return new ConstraintMetadata(rowConstraints, tableConstraints, foreignKeyConstraints);
        }
    }
}
