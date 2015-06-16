// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.keyvalue.impl;

import java.util.Arrays;

import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;

public class RowWrapper implements Comparable<RowWrapper> {
    final byte[] row;

    public RowWrapper(byte[] row) {
        Cell.validateNameValid(row);
        this.row = row;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(row);
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
        RowWrapper other = (RowWrapper)obj;
        if (!Arrays.equals(row, other.row))
            return false;
        return true;
    }

    @Override
    public int compareTo(RowWrapper o) {
        return UnsignedBytes.lexicographicalComparator().compare(row, o.row);
    }
}
