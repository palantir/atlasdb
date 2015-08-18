/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.api;

import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Defaults;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.UnsignedBytes;

/**
 * Represents a cell in the key-value store.
 * @see Value
 * @see Bytes
 */
public final class Cell implements Serializable, Comparable<Cell> {
    private static final long serialVersionUID = 1L;

    // Oracle has an upper bound on RAW types of 2000.
    public static final int MAX_NAME_LENGTH = 1500;

    // these /have/ to be these values to retain serialization back-compat
    public static final long INVALID_TTL = Defaults.defaultValue(long.class);
    public static final TimeUnit INVALID_TTL_TYPE = null;

    /**
     * Creates a key. Do not modify the rowName or the columnName arrays after passing them.
     * This doesn't make a copy for performance reasons.
     */
    public static Cell create(byte[] rowName, byte[] columnName) {
        return new Cell(rowName, columnName, INVALID_TTL);
    }

    public static Cell create(byte[] rowName, byte[] columnName, long ttlDuration, TimeUnit ttlUnit) {
        return new Cell(rowName, columnName, safeTimeConvert(ttlDuration, ttlUnit));
    }

    public static boolean isNameValid(byte[] name) {
        return name != null && name.length >0 && name.length <= MAX_NAME_LENGTH;
    }

    public static void validateNameValid(byte[] name) {
        Preconditions.checkNotNull(name, "name cannot be null");
        Preconditions.checkArgument(name.length > 0, "name must be non-empty");
        Preconditions.checkArgument(name.length <= MAX_NAME_LENGTH, "name must be no larger than " + MAX_NAME_LENGTH);
    }

    private final byte[] rowName;
    private final byte[] columnName;
    private final long ttlDurationMillis;

    private Cell(byte[] rowName, byte[] columnName) {
        this(rowName, columnName, INVALID_TTL);
    }

    // NOTE: This constructor doesn't copy the arrays for performance reasons.
    @JsonCreator
    private Cell(@JsonProperty("rowName") byte[] rowName,
                 @JsonProperty("columnName") byte[] columnName,
                 @JsonProperty("ttlDurationMillis") long ttlDurationMillis) {
        validateNameValid(rowName);
        validateNameValid(columnName);
        this.rowName = rowName;
        this.columnName = columnName;
        this.ttlDurationMillis = ttlDurationMillis;
    }

    /**
     * The name of the row within the table.
     */
    @Nonnull public byte[] getRowName() {
        return rowName;
    }

    /**
     * The name of the column within the row.
     */
    @Nonnull public byte[] getColumnName() {
        return columnName;
    }


    public long getTtlDurationMillis() {
        return ttlDurationMillis;
    }

    private static long safeTimeConvert(long ttl, TimeUnit ttlUnit) {
        if (ttlUnit != null) {
            return TimeUnit.MILLISECONDS.convert(ttl, ttlUnit);
        } else {
            return 0;
        }
    }

    public static final Function<Cell, Boolean> IS_EXPIRING = new Function<Cell, Boolean>() {
        @Override
        public Boolean apply(Cell from) {
            return from.getTtlDurationMillis() != INVALID_TTL;
        }
    };

    @Override
    public int compareTo(Cell o) {
        int cmp = UnsignedBytes.lexicographicalComparator().compare(rowName, o.rowName);
        if (cmp != 0) { return cmp; }
        return UnsignedBytes.lexicographicalComparator().compare(columnName, o.columnName);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Cell)) {
            return false;
        }
        Cell other = (Cell)obj;
        return Arrays.equals(rowName, other.rowName)
                && Arrays.equals(columnName, other.columnName);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(rowName) ^ Arrays.hashCode(columnName);
    }

    @Override
    public String toString() {
        return "Cell [rowName=" + getNameFromBytes(rowName) + ", columnName=" +
                getNameFromBytes(columnName) + "]";
    }

    public static String getNameFromBytes(byte[] name) {
        return BaseEncoding.base16().lowerCase().encode(name);
    }
}
