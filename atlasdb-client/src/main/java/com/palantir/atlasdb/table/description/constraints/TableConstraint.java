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

public enum TableConstraint implements Constraint {
    /**
     * This is a constraint defined as: once a row has been added, no cells in that row
     * may be added or modified in future transactions. If
     * an attempt is made to overwrite an existing key/row, then the constraint will fail.
     */
    UNIQUE_KEY {
        @Override
        public String getMethodName() {
            return "checkUniqueKey";
        }
    },

    /**
     * This is a constraint defined as: no cell (key/column pair) can be overwritten in this table. If
     * an attempt is made to overwrite an existing cell, then the constraint will fail.
     */
    UNIQUE_CELL {
        @Override
        public String getMethodName() {
            return "checkUniqueCell";
        }
    };

    public abstract String getMethodName();
}
