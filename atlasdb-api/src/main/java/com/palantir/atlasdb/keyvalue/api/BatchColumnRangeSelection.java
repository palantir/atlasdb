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
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.encoding.PtBytes;
import java.io.Serializable;
import java.util.regex.Pattern;

public final class BatchColumnRangeSelection implements Serializable {
    private static final long serialVersionUID = 1L;

    private final ColumnRangeSelection columnRangeSelection;
    private final int batchHint;

    private BatchColumnRangeSelection(ColumnRangeSelection columnRangeSelection, int batchHint) {
        this.columnRangeSelection = columnRangeSelection;
        this.batchHint = batchHint;
    }

    public static BatchColumnRangeSelection create(ColumnRangeSelection columnRangeSelection, int batchHint) {
        return new BatchColumnRangeSelection(columnRangeSelection, batchHint);
    }

    @JsonCreator
    public static BatchColumnRangeSelection create(
            @JsonProperty("startCol") byte[] startCol,
            @JsonProperty("endCol") byte[] endCol,
            @JsonProperty("batchHint") int batchHint) {
        return new BatchColumnRangeSelection(new ColumnRangeSelection(startCol, endCol), batchHint);
    }

    public byte[] getStartCol() {
        return columnRangeSelection.getStartCol();
    }

    public byte[] getEndCol() {
        return columnRangeSelection.getEndCol();
    }

    public int getBatchHint() {
        return batchHint;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + batchHint;
        result = prime * result + ((columnRangeSelection == null) ? 0 : columnRangeSelection.hashCode());
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
        if (getClass() != obj.getClass()) {
            return false;
        }
        BatchColumnRangeSelection other = (BatchColumnRangeSelection) obj;
        if (batchHint != other.batchHint) {
            return false;
        }
        if (columnRangeSelection == null) {
            if (other.columnRangeSelection != null) {
                return false;
            }
        } else if (!columnRangeSelection.equals(other.columnRangeSelection)) {
            return false;
        }
        return true;
    }

    private static final Pattern deserializeRegex = Pattern.compile("\\s*,\\s*");

    @SuppressWarnings("StringSplitter") // Uncertainty around performance implications
    public static BatchColumnRangeSelection valueOf(String serialized) {
        String[] split = deserializeRegex.split(serialized);
        byte[] startCol = PtBytes.decodeBase64(split[0]);
        byte[] endCol = PtBytes.decodeBase64(split[1]);
        int batchHint = Integer.parseInt(split[2]);
        return BatchColumnRangeSelection.create(startCol, endCol, batchHint);
    }

    @Override
    public String toString() {
        String start = PtBytes.encodeBase64String(getStartCol());
        String end = PtBytes.encodeBase64String(getEndCol());
        String batch = String.valueOf(batchHint);
        return Joiner.on(',').join(ImmutableList.of(start, end, batch));
    }
}
