/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.jdbc.impl;

import java.util.Arrays;

public class CellTimestamp {
    private final byte[] rowName;
    private final byte[] colName;
    private final long timestamp;

    public CellTimestamp(byte[] rowName, byte[] colName, long timestamp) {
        this.rowName = rowName;
        this.colName = colName;
        this.timestamp = timestamp;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(colName);
        result = prime * result + Arrays.hashCode(rowName);
        result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof CellTimestamp)) {
            return false;
        }
        CellTimestamp other = (CellTimestamp) obj;
        if (!Arrays.equals(colName, other.colName)) {
            return false;
        }
        if (!Arrays.equals(rowName, other.rowName)) {
            return false;
        }
        if (timestamp != other.timestamp) {
            return false;
        }
        return true;
    }
}
