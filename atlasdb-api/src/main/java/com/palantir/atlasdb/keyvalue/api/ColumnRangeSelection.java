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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.encoding.PtBytes;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Pattern;

public class ColumnRangeSelection implements Serializable {
    private static final long serialVersionUID = 1L;

    // Inclusive start column name.
    private final byte[] startCol;
    // Exclusive end column name.
    private final byte[] endCol;

    @JsonCreator
    public ColumnRangeSelection(@JsonProperty("startCol") byte[] startCol,
                                @JsonProperty("endCol") byte[] endCol) {
        this.startCol = MoreObjects.firstNonNull(startCol, PtBytes.EMPTY_BYTE_ARRAY);
        this.endCol = MoreObjects.firstNonNull(endCol, PtBytes.EMPTY_BYTE_ARRAY);
        Preconditions.checkArgument(isValidRange(this.startCol, this.endCol),
                "Start and end columns (%s, %s respectively) do not form a valid range.",
                startCol,
                endCol);
    }

    public byte[] getStartCol() {
        return startCol;
    }

    public byte[] getEndCol() {
        return endCol;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ColumnRangeSelection that = (ColumnRangeSelection) obj;
        return Arrays.equals(startCol, that.startCol)
                && Arrays.equals(endCol, that.endCol);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(startCol), Arrays.hashCode(endCol));
    }

    private static final Pattern deserializeRegex = Pattern.compile("\\s*,\\s*");

    public static ColumnRangeSelection valueOf(String serialized) {
        // Pass in -1 to split so that it doesn't discard empty strings
        String[] split = deserializeRegex.split(serialized, -1);
        byte[] startCol = PtBytes.decodeBase64(split[0]);
        byte[] endCol = PtBytes.decodeBase64(split[1]);
        return new ColumnRangeSelection(startCol, endCol);
    }

    @Override
    public String toString() {
        String start = PtBytes.encodeBase64String(startCol);
        String end = PtBytes.encodeBase64String(endCol);
        return Joiner.on(',').join(ImmutableList.of(start, end));
    }

    public static boolean isValidRange(byte[] startCol, byte[] endCol) {
        return RangeRequests.isContiguousRange(false, startCol, endCol)
                && !RangeRequests.isExactlyEmptyRange(startCol, endCol);
    }
}
