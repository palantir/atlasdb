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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.common.annotation.Immutable;
import com.palantir.common.persist.Persistable;
import com.palantir.logsafe.Preconditions;
import com.palantir.util.Pair;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Allows you to restrict a call on the database
 * to specific rows and columns.
 * By default, calls on a table in the key-value store
 * cover all rows and columns in the table.
 * To restrict the rows or columns,  call
 * the methods on the <code>RangeRequest</code> class.
 */
@Immutable
public final class RangeRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    private final byte[] startInclusive;
    private final byte[] endExclusive;
    private final ImmutableSortedSet<byte[]> columns;
    private final Integer batchHint;
    private final boolean reverse;
    private transient int hashCode = 0;

    /**
     * Returns a {@link Builder} instance, a helper class
     * for instantiating immutable <code>RangeRequest</code>
     * objects.
     */
    public static Builder builder() {
        return new Builder(false);
    }

    public static Builder builder(boolean reverse) {
        return new Builder(reverse);
    }

    public static Builder reverseBuilder() {
        return new Builder(true);
    }

    public static RangeRequest all() {
        return builder().build();
    }

    @JsonCreator
    private RangeRequest(
            @JsonProperty("startInclusive") byte[] startInclusive,
            @JsonProperty("endExclusive") byte[] endExclusive,
            @JsonProperty("columnNames") Iterable<byte[]> cols,
            @JsonProperty("batchHint") Integer batchHint,
            @JsonProperty("reverse") boolean reverse) {
        this.startInclusive = startInclusive;
        this.endExclusive = endExclusive;
        this.columns = cloneSet(cols);
        this.batchHint = batchHint;
        this.reverse = reverse;
    }

    /**
     * Start is inclusive.  If this range is reversed, then the start will
     * be after the end.
     * <p>
     * This array may be empty if the start is unbounded.
     */
    @Nonnull
    public byte[] getStartInclusive() {
        return startInclusive.clone();
    }

    /**
     * End is exclusive.  If this range is reversed, then the end will
     * be before the start.
     * <p>
     * This array may be empty if the end doens't have a bound.
     */
    @Nonnull
    public byte[] getEndExclusive() {
        return endExclusive.clone();
    }

    /**
     * An empty set of column names means that all columns are selected.
     */
    @Nonnull
    public SortedSet<byte[]> getColumnNames() {
        return columns;
    }

    public boolean containsColumn(byte[] col) {
        return columns.isEmpty() || columns.contains(col);
    }

    public boolean isReverse() {
        return reverse;
    }

    @JsonIgnore
    public boolean isEmptyRange() {
        if (startInclusive.length == 0 && RangeRequests.isFirstRowName(reverse, endExclusive)) {
            return true;
        }
        if (startInclusive.length == 0 || endExclusive.length == 0) {
            return false;
        }
        if (reverse) {
            return UnsignedBytes.lexicographicalComparator().compare(startInclusive, endExclusive) <= 0;
        } else {
            return UnsignedBytes.lexicographicalComparator().compare(startInclusive, endExclusive) >= 0;
        }
    }

    public boolean inRange(byte[] position) {
        Preconditions.checkArgument(Cell.isNameValid(position));
        final boolean afterStart;
        final boolean afterEnd;
        if (reverse) {
            afterStart = getStartInclusive().length == 0
                    || UnsignedBytes.lexicographicalComparator().compare(getStartInclusive(), position) >= 0;
            afterEnd = getEndExclusive().length == 0
                    || UnsignedBytes.lexicographicalComparator().compare(getEndExclusive(), position) < 0;
        } else {
            afterStart = getStartInclusive().length == 0
                    || UnsignedBytes.lexicographicalComparator().compare(getStartInclusive(), position) <= 0;
            afterEnd = getEndExclusive().length == 0
                    || UnsignedBytes.lexicographicalComparator().compare(getEndExclusive(), position) > 0;
        }

        return afterStart && afterEnd;
    }

    @Nullable
    public Integer getBatchHint() {
        return batchHint;
    }

    public RangeRequest withBatchHint(int hint) {
        return new RangeRequest(startInclusive, endExclusive, columns, hint, reverse);
    }

    @JsonIgnore
    public Builder getBuilder() {
        return new Builder(reverse)
                .endRowExclusive(endExclusive)
                .startRowInclusive(startInclusive)
                .batchHint(batchHint)
                .retainColumns(columns);
    }

    @Override
    public String toString() {
        ToStringHelper helper = MoreObjects.toStringHelper(getClass()).omitNullValues();
        PtBytes.addIfNotEmpty(helper, "startInclusive", startInclusive);
        PtBytes.addIfNotEmpty(helper, "endExclusive", endExclusive);
        if (columns != null && !columns.isEmpty()) {
            helper.add(
                    "columns",
                    FluentIterable.from(columns).filter(Predicates.notNull()).transform(PtBytes.BYTES_TO_HEX_STRING));
        }
        helper.add("batchHint", batchHint);
        helper.add("reverse", reverse);
        return helper.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RangeRequest that = (RangeRequest) obj;
        return reverse == that.reverse
                && Arrays.equals(startInclusive, that.startInclusive)
                && Arrays.equals(endExclusive, that.endExclusive)
                && Objects.equals(columns, that.columns)
                && Objects.equals(batchHint, that.batchHint);
    }

    @Override
    public int hashCode() {
        /*
         * Lazily compute and store hashcode since instances are frequently
         * accessed via hash collections, but computation can be expensive, and
         * allow for benign data races.
         */
        if (hashCode == 0) {
            hashCode = Objects.hash(
                    Arrays.hashCode(startInclusive), Arrays.hashCode(endExclusive), columns, batchHint, reverse);
        }
        return hashCode;
    }

    private static ImmutableSortedSet<byte[]> cloneSet(Iterable<byte[]> set) {
        ImmutableSortedSet.Builder<byte[]> builder =
                ImmutableSortedSet.orderedBy(UnsignedBytes.lexicographicalComparator());
        for (byte[] col : set) {
            builder.add(col.clone());
        }
        return builder.build();
    }

    /**
     * This will return a start and end row that will exactly contain all rows for this prefix in
     * reverse.
     * <p>
     * start will be on the left hand side and will be greater lexicographically
     */
    private static Pair<byte[], byte[]> createNamesForReversePrefixScan(@Nonnull byte[] name) {
        Preconditions.checkNotNull(name, "name cannot be null");
        Preconditions.checkArgument(name.length <= Cell.MAX_NAME_LENGTH, "name is too long");

        if (name.length == 0) {
            return Pair.create(name, name);
        }
        byte[] startName = new byte[Cell.MAX_NAME_LENGTH];
        System.arraycopy(name, 0, startName, 0, name.length);
        for (int i = name.length; i < startName.length; i++) {
            startName[i] = (byte) 0xff;
        }
        byte[] endName = RangeRequests.previousLexicographicName(name);
        return Pair.create(startName, endName);
    }

    /**
     * A helper class used to construct an immutable {@link RangeRequest}
     * instance.
     * <p>
     * By default, the range covers all rows and columns. To restrict the rows or columns,
     * call      * the methods on the <code>RangeRequest</code> class.
     */
    @NotThreadSafe
    public static final class Builder {
        private byte[] startInclusive = PtBytes.EMPTY_BYTE_ARRAY;
        private byte[] endExclusive = PtBytes.EMPTY_BYTE_ARRAY;
        private Set<byte[]> columns = new TreeSet<>(UnsignedBytes.lexicographicalComparator());
        private Integer batchHint = null;
        private final boolean reverse;

        Builder(boolean reverse) {
            this.reverse = reverse;
        }

        public boolean isReverse() {
            return reverse;
        }

        /**
         * This will set the start and the end to get all rows that have a given prefix.
         */
        public Builder prefixRange(byte[] prefix) {
            Preconditions.checkNotNull(prefix, "prefix cannot be null");

            if (reverse) {
                Pair<byte[], byte[]> pair = createNamesForReversePrefixScan(prefix);
                this.startInclusive = pair.lhSide;
                this.endExclusive = pair.rhSide;
            } else {
                this.startInclusive = prefix.clone();
                this.endExclusive = RangeRequests.createEndNameForPrefixScan(prefix);
            }

            return this;
        }

        public Builder startRowInclusive(byte[] start) {
            this.startInclusive =
                    Preconditions.checkNotNull(start, "start cannot be null").clone();
            return this;
        }

        public Builder startRowInclusive(Prefix start) {
            return startRowInclusive(start.getBytes());
        }

        public Builder startRowInclusive(Persistable start) {
            return startRowInclusive(start.persistToBytes());
        }

        public Builder endRowExclusive(byte[] end) {
            this.endExclusive =
                    Preconditions.checkNotNull(end, "end cannot be null").clone();
            return this;
        }

        public Builder endRowExclusive(Prefix end) {
            return endRowExclusive(end.getBytes());
        }

        public Builder endRowExclusive(Persistable end) {
            return endRowExclusive(end.persistToBytes());
        }

        public Builder retainColumns(Iterable<byte[]> colsToRetain) {
            Iterables.addAll(columns, colsToRetain);
            return this;
        }

        public Builder retainColumns(ColumnSelection selection) {
            if (!selection.allColumnsSelected()) {
                Iterables.addAll(columns, selection.getSelectedColumns());
            }
            return this;
        }

        /**
         * This is a hint for how much data the underlying system should process at a time.  If we are expecting to
         * read a lot from this range, then this should be pretty large for performance.  If we are only going to read
         * the first thing in a range, then this should be set to 1.
         * <p>
         * If hint is null then the range will use the default. Usually for
         * {@link Transaction#getRange(TableReference, RangeRequest)}
         * this means the batch size will be whatever is passed as the batch size to
         * BatchingVisitable#batchAccept(int, com.palantir.common.base.AbortingVisitor)
         */
        public Builder batchHint(Integer hint) {
            Preconditions.checkArgument(hint == null || hint > 0);
            batchHint = hint;
            return this;
        }

        public boolean isInvalidRange() {
            return !RangeRequests.isContiguousRange(reverse, startInclusive, endExclusive);
        }

        public RangeRequest build() {
            RangeRequest rangeRequest = new RangeRequest(startInclusive, endExclusive, columns, batchHint, reverse);
            if (isInvalidRange()) {
                throw new IllegalArgumentException(
                        "Invalid range request, check row byte ordering for reverse ordered values: " + rangeRequest);
            }
            return rangeRequest;
        }
    }
}
