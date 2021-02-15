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
package com.palantir.atlasdb.table.description;

import com.palantir.atlasdb.table.description.render.Renderers;
import com.palantir.logsafe.Preconditions;

public final class IndexComponent {
    final NameComponentDescription rowKeyDesc;

    // getting data from the row key
    final String rowComponentName;

    // getting data from the dynamic column name
    final String dynamicColumnComponentName;

    // getting data from a column
    final String columnNameToGetData;
    final String codeToAccessValue;

    final boolean isMultiple;

    /**
     * This takes an existing row component and uses it in the index.
     */
    public static IndexComponent createFromRow(NameComponentDescription rowKeyDesc, String rowComponentName) {
        return new IndexComponent(
                Preconditions.checkNotNull(rowKeyDesc),
                Preconditions.checkNotNull(rowComponentName),
                null,
                null,
                null,
                false);
    }

    /**
     * This takes an existing dynamic column component and uses it in the index.
     */
    public static IndexComponent createFromDynamicColumn(
            NameComponentDescription rowKeyDesc, String dynamicColumnComponentName) {
        return new IndexComponent(
                Preconditions.checkNotNull(rowKeyDesc),
                null,
                Preconditions.checkNotNull(dynamicColumnComponentName),
                null,
                null,
                false);
    }

    /**
     * This takes a value from a column and uses it in the index .
     *
     * @param rowKeyDesc this is the data type of this component of the index
     * @param columnNameToGetData this is the column that we are making an index over
     * @param codeToAccessValue this is code snippet to be run to extract the value of a column.
     *          The string "_value" can be used in this string as the value type of the column
     */
    public static IndexComponent createFromColumn(
            NameComponentDescription rowKeyDesc, String columnNameToGetData, String codeToAccessValue) {

        return new IndexComponent(
                Preconditions.checkNotNull(rowKeyDesc),
                null,
                null,
                Preconditions.checkNotNull(columnNameToGetData),
                Preconditions.checkNotNull(codeToAccessValue),
                false);
    }

    /**
     * This takes a value from a column and creates a variable number of rows.  The code should return an Iterable of
     * the expected type.
     *
     * @param rowKeyDesc this is the data type of this component of the index
     * @param columnNameToGetData this is the column that we are making an index over
     * @param codeToAccessValue this is code snippet to be run to extract the value of a column.
     *          The string "_value" can be used in this string as the value type of the column
     */
    public static IndexComponent createIterableFromColumn(
            NameComponentDescription rowKeyDesc, String columnNameToGetData, String codeToAccessValue) {

        return new IndexComponent(
                Preconditions.checkNotNull(rowKeyDesc),
                null,
                null,
                Preconditions.checkNotNull(columnNameToGetData),
                Preconditions.checkNotNull(codeToAccessValue),
                true);
    }

    private IndexComponent(
            NameComponentDescription rowKeyDesc,
            String rowComponentName,
            String dynamicColumnComponentName,
            String columnNameToGetData,
            String codeToAccessValue,
            boolean isMultiple) {
        this.rowKeyDesc = rowKeyDesc;
        this.rowComponentName = rowComponentName;
        this.dynamicColumnComponentName = dynamicColumnComponentName;
        this.columnNameToGetData = columnNameToGetData;
        this.codeToAccessValue = codeToAccessValue;
        this.isMultiple = isMultiple;
    }

    public IndexComponent withPartitioners(RowNamePartitioner... partitioners) {
        NameComponentDescription newRowKeyDesc = rowKeyDesc.withPartitioners(partitioners);
        return new IndexComponent(
                newRowKeyDesc,
                rowComponentName,
                dynamicColumnComponentName,
                columnNameToGetData,
                codeToAccessValue,
                isMultiple);
    }

    public NameComponentDescription getRowKeyDescription() {
        return rowKeyDesc;
    }

    public String getValueCode(String rowCode, String dynamicColCode, String columnCode) {
        if (rowComponentName != null) {
            return rowCode + ".get" + Renderers.CamelCase(rowComponentName) + "()";
        } else if (dynamicColumnComponentName != null) {
            Preconditions.checkArgument(dynamicColCode != null, "cannot apply to non dynamic table.");
            return dynamicColCode + ".get" + Renderers.CamelCase(dynamicColumnComponentName) + "()";
        } else {
            return codeToAccessValue.replaceAll("_value", columnCode);
        }
    }

    public boolean isMultiple() {
        return isMultiple;
    }
}
