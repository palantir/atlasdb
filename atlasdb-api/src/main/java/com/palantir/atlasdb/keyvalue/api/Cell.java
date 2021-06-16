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
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a cell in the key-value store.
 * @see Value
 * @see Bytes
 */
public final class Cell implements Serializable, Comparable<Cell> {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(Cell.class);

    // Oracle has an upper bound on RAW types of 2000.
    public static final int MAX_NAME_LENGTH = 1500;
    public static final Comparator<Cell> COLUMN_COMPARATOR = PtBytes.BYTES_COMPARATOR.onResultOf(Cell::getColumnName);

    /**
     * Creates a key. Do not modify the rowName or the columnName arrays after passing them.
     * This doesn't make a copy for performance reasons.
     */
    public static Cell create(byte[] rowName, byte[] columnName) {
        return new Cell(rowName, columnName);
    }

    public static boolean isNameValid(byte[] name) {
        return name != null && name.length > 0 && name.length <= MAX_NAME_LENGTH;
    }

    private void validateNameValid(byte[] name) {
        com.palantir.logsafe.Preconditions.checkNotNull(name, "name cannot be null");
        com.palantir.logsafe.Preconditions.checkArgument(name.length > 0, "name must be non-empty");

        try {
            Preconditions.checkArgument(
                    name.length <= MAX_NAME_LENGTH, "name must be no longer than %s.", MAX_NAME_LENGTH);
        } catch (IllegalArgumentException e) {
            log.error(
                    "Cell name length exceeded. Name must be no longer than {}. "
                            + "Cell creation that was attempted was: {}; since the vast majority of people "
                            + "encountering this problem are using unbounded Strings as components, it may aid your "
                            + "debugging to know the UTF-8 interpretation of the bad field was: [{}]",
                    SafeArg.of("max name length", MAX_NAME_LENGTH),
                    UnsafeArg.of("cell", this),
                    UnsafeArg.of("name", new String(name, StandardCharsets.UTF_8)),
                    e);
            throw e;
        }
    }

    private final byte[] rowName;
    private final byte[] columnName;
    private transient int hashCode = 0;

    // NOTE: This constructor doesn't copy the arrays for performance reasons.
    @JsonCreator
    private Cell(@JsonProperty("rowName") byte[] rowName, @JsonProperty("columnName") byte[] columnName) {
        this.rowName = rowName;
        this.columnName = columnName;

        validateNameValid(rowName);
        validateNameValid(columnName);
    }

    /**
     * The name of the row within the table.
     */
    @Nonnull
    public byte[] getRowName() {
        return rowName;
    }

    /**
     * The name of the column within the row.
     */
    @Nonnull
    public byte[] getColumnName() {
        return columnName;
    }

    @Override
    public int compareTo(Cell other) {
        int cmp = UnsignedBytes.lexicographicalComparator().compare(rowName, other.rowName);
        if (cmp != 0) {
            return cmp;
        }
        return UnsignedBytes.lexicographicalComparator().compare(columnName, other.columnName);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Cell)) {
            return false;
        }
        Cell other = (Cell) obj;
        return Arrays.equals(rowName, other.rowName) && Arrays.equals(columnName, other.columnName);
    }

    @Override
    public int hashCode() {
        /*
         * Lazily compute and store hashcode since instances are frequently
         * accessed via hash collections, but computation can be expensive, and
         * allow for benign data races.
         */
        if (hashCode == 0) {
            hashCode = Arrays.hashCode(rowName) ^ Arrays.hashCode(columnName);
        }
        return hashCode;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass())
                .add("rowName", PtBytes.encodeHexString(rowName))
                .add("columnName", PtBytes.encodeHexString(columnName))
                .toString();
    }
}
