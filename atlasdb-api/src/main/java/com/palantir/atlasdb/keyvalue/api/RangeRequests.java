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

import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class RangeRequests {
    private RangeRequests() {
        // Utility class
    }

    /**
     * This will return the first byte[] less than the given byte[] lexicographically.
     * <p>
     * If name is the array <code> new byte[] { 0 }</code> an empty byte[] will be returned.
     * <p>
     * This method will throw if the empty byte array is passed. It will also reject arrays with
     * length greater than <code>Cell.MAX_NAME_LENGTH</code>
     */
    public static byte[] previousLexicographicName(@Nonnull byte[] name) {
        Preconditions.checkArgument(name.length <= Cell.MAX_NAME_LENGTH, "name is too long");
        Preconditions.checkArgument(name.length > 0, "name is empty");
        if (name[name.length - 1] == 0) {
            byte[] ret = new byte[name.length - 1];
            System.arraycopy(name, 0, ret, 0, ret.length);
            return ret;
        }
        byte[] ret = new byte[Cell.MAX_NAME_LENGTH];
        System.arraycopy(name, 0, ret, 0, name.length);
        ret[name.length - 1]--;
        for (int i = name.length; i < ret.length; i++) {
            ret[i] = (byte) 0xff;
        }
        return ret;
    }

    /**
     * This will return the row name that will include exactly all prefix matches if passed to
     * {@link KeyValueService#getRange(TableReference, RangeRequest, long)}.
     * <p>
     * @param rowName must be non-null
     */
    public static byte[] createEndNameForPrefixScan(@Nonnull byte[] rowName) {
        Preconditions.checkNotNull(rowName, "name cannot be null");
        Preconditions.checkArgument(rowName.length <= Cell.MAX_NAME_LENGTH, "name is too long");

        for (int i = rowName.length - 1; i >= 0; i--) {
            if ((rowName[i] & 0xff) != 0xff) {
                byte[] ret = new byte[i + 1];
                System.arraycopy(rowName, 0, ret, 0, i + 1);
                ret[i]++;
                return ret;
            }
        }

        return PtBytes.EMPTY_BYTE_ARRAY;
    }

    public static byte[] nextLexicographicName(@Nonnull byte[] name) {
        byte[] ret = nextLexicographicNameInternal(name);
        if (ret == null) {
            throw new SafeIllegalArgumentException("Name is lexicographically maximum and cannot be incremented.");
        }
        return ret;
    }

    static final byte[] ONE_AFTER_MAXIMUM_NAME;

    static {
        ONE_AFTER_MAXIMUM_NAME = new byte[Cell.MAX_NAME_LENGTH + 1];
        for (int i = 0; i < Cell.MAX_NAME_LENGTH; ++i) {
            ONE_AFTER_MAXIMUM_NAME[i] = (byte) 0xff;
        }
        ONE_AFTER_MAXIMUM_NAME[Cell.MAX_NAME_LENGTH] = (byte) 0x00;
    }

    public static byte[] oneAfterMaximumName() {
        return ONE_AFTER_MAXIMUM_NAME;
    }

    /**
     * This is a replacement for endRow when doing a non-reverse range request.
     */
    public static byte[] endRowExclusiveOrOneAfterMax(RangeRequest rangeRequest) {
        Preconditions.checkArgument(!rangeRequest.isReverse());
        if (rangeRequest.getEndExclusive().length == 0) {
            return oneAfterMaximumName();
        }
        return rangeRequest.getEndExclusive();
    }

    /**
     * This is a replacement for startRow when doing reverse range request.
     */
    public static byte[] startRowInclusiveOrLargestRow(RangeRequest rangeRequest) {
        Preconditions.checkArgument(rangeRequest.isReverse());
        if (rangeRequest.getStartInclusive().length == 0) {
            return getLastRowName();
        }
        return rangeRequest.getStartInclusive();
    }

    public static ColumnSelection extractColumnSelection(RangeRequest rangeRequest) {
        if (rangeRequest.getColumnNames().isEmpty()) {
            return ColumnSelection.all();
        }
        return ColumnSelection.create(rangeRequest.getColumnNames());
    }

    private static byte[] nextLexicographicNameInternal(@Nonnull byte[] name) {
        Preconditions.checkArgument(name.length <= Cell.MAX_NAME_LENGTH, "name is too long");
        if (name.length < Cell.MAX_NAME_LENGTH) {
            byte[] ret = new byte[name.length + 1];
            System.arraycopy(name, 0, ret, 0, name.length);
            ret[name.length] = (byte) 0;
            return ret;
        } else {
            for (int i = name.length - 1; i >= 0; i--) {
                if ((name[i] & 0xff) != 0xff) {
                    byte[] ret = new byte[i + 1];
                    System.arraycopy(name, 0, ret, 0, i + 1);
                    ret[i]++;
                    return ret;
                }
            }
            return null;
        }
    }

    public static byte[] getFirstRowName() {
        return new byte[] {0};
    }

    public static byte[] getLastRowName() {
        byte[] ret = new byte[Cell.MAX_NAME_LENGTH];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = (byte) 0xff;
        }
        return ret;
    }

    public static boolean isFirstRowName(boolean reverse, @Nonnull byte[] name) {
        return reverse ? isLastRowName(name) : isFirstRowName(name);
    }

    public static boolean isFirstRowName(@Nonnull byte[] name) {
        return name.length == 1 && name[0] == 0;
    }

    public static boolean isLastRowName(@Nonnull byte[] name) {
        if (name.length < Cell.MAX_NAME_LENGTH) {
            return false;
        }
        return nextLexicographicNameInternal(name) == null;
    }

    public static boolean isTerminalRow(boolean reverse, @Nonnull byte[] rowName) {
        if (reverse) {
            return isFirstRowName(rowName);
        } else {
            return isLastRowName(rowName);
        }
    }

    public static byte[] getNextStartRow(boolean reverse, @Nonnull byte[] rowName) {
        if (reverse) {
            return previousLexicographicName(rowName);
        } else {
            return nextLexicographicName(rowName);
        }
    }

    @Nullable
    public static byte[] getNextStartRowUnlessTerminal(boolean reverse, @Nonnull byte[] rowName) {
        if (reverse) {
            if (isFirstRowName(rowName)) {
                return null;
            } else {
                return previousLexicographicName(rowName);
            }
        } else {
            return nextLexicographicNameInternal(rowName);
        }
    }

    public static boolean isContiguousRange(boolean reverse, byte[] startInclusive, byte[] endExclusive) {
        if (startInclusive.length == 0 || endExclusive.length == 0) {
            return true;
        }
        if (reverse) {
            return UnsignedBytes.lexicographicalComparator().compare(startInclusive, endExclusive) >= 0;
        }
        return UnsignedBytes.lexicographicalComparator().compare(startInclusive, endExclusive) <= 0;
    }

    public static boolean isExactlyEmptyRange(byte[] startInclusive, byte[] endExclusive) {
        if (startInclusive.length == 0 || endExclusive.length == 0) {
            return false;
        }
        return UnsignedBytes.lexicographicalComparator().compare(startInclusive, endExclusive) == 0;
    }

    public static RangeRequest ofSingleRow(@Nonnull byte[] row) {
        return RangeRequest.builder()
                .startRowInclusive(row)
                .endRowExclusive(RangeRequests.nextLexicographicName(row))
                .build();
    }
}
