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

package com.palantir.atlasdb.keyvalue.cassandra.paging;

import java.util.Arrays;

import org.apache.cassandra.thrift.Column;

public class CassandraRawCellValue {
    private final byte[] rowKey;
    private final Column column;

    public CassandraRawCellValue(byte[] rowKey, Column column) {
        this.rowKey = rowKey;
        this.column = column;
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public Column getColumn() {
        return column;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        CassandraRawCellValue that = (CassandraRawCellValue) other;

        if (!Arrays.equals(rowKey, that.rowKey)) {
            return false;
        }
        return column != null ? column.equals(that.column) : that.column == null;
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(rowKey);
        result = 31 * result + (column != null ? column.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "CassandraRawCellValue{" + "rowKey=" + Arrays.toString(rowKey) + ", column=" + column + '}';
    }
}
