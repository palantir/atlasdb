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
import com.google.common.primitives.Bytes;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import javax.annotation.Nonnull;

/**
 * Represents a cell in the key-value store.
 * @see Value
 * @see Bytes
 */
public final class Cell implements Serializable, Comparable<Cell> {
    private static final long serialVersionUID = 1L;
    private static final SafeLogger log = SafeLoggerFactory.get(Cell.class);

    // Oracle has an upper bound on RAW types of 2000.
    public static final int MAX_NAME_LENGTH = 1500;
    private static final SafeArg<Integer> MAX_NAME_LENGTH_ARG = SafeArg.of("maxNameLength", MAX_NAME_LENGTH);

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

    private byte[] validateNameValid(byte[] name) {
        if (isNameValid(name)) {
            return name;
        }
        throw invalidName(name);
    }

    /**
     * Returns exception for invalid Cell name.
     * Intentionally pulled out of the happy-path hot path for valid Cell names.
     */
    private SafeIllegalArgumentException invalidName(byte[] name) {
        Preconditions.checkNotNull(name, "name cannot be null");
        Preconditions.checkArgument(name.length > 0, "name must be non-empty");
        SafeIllegalArgumentException exception =
                new SafeIllegalArgumentException("name length exceeds maximum", MAX_NAME_LENGTH_ARG);
        log.error(
                "Cell name length exceeded. Name must be no longer than {}. "
                        + "Cell creation that was attempted was: {}; since the vast majority of people "
                        + "encountering this problem are using unbounded Strings as components, it may aid your "
                        + "debugging to know the UTF-8 interpretation of the bad field was: [{}]",
                MAX_NAME_LENGTH_ARG,
                UnsafeArg.of("cell", this),
                UnsafeArg.of("name", new String(name, StandardCharsets.UTF_8)),
                exception);
        return exception;
    }

    private final byte[] rowName;
    private final byte[] columnName;
    private transient int hashCode = 0;

    // NOTE: This constructor doesn't copy the arrays for performance reasons.
    @JsonCreator
    private Cell(@JsonProperty("rowName") byte[] rowName, @JsonProperty("columnName") byte[] columnName) {
        this.rowName = validateNameValid(rowName);
        this.columnName = validateNameValid(columnName);
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
        return this.hashCode() == other.hashCode()
                && Arrays.equals(rowName, other.rowName)
                && Arrays.equals(columnName, other.columnName);
    }

    @Override
    public int hashCode() {
        /*
         * Lazily compute and store hashcode since instances are frequently
         * accessed via hash collections, but computation can be expensive, and
         * allow for benign data races.
         */
        if (hashCode == 0) {
            hashCode = 31 * Arrays.hashCode(rowName) + Arrays.hashCode(columnName);
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
