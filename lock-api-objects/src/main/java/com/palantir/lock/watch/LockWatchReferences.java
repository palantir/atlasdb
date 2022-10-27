/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.watch;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.Range;
import com.palantir.common.annotations.ImmutablesStyles.PackageVisibleImmutablesStyle;
import com.palantir.lock.AtlasLockDescriptorRanges;
import com.palantir.lock.LockDescriptor;
import org.immutables.value.Value;

public final class LockWatchReferences {
    public static final LockDescriptorRangeVisitor TO_RANGES_VISITOR = new LockDescriptorRangeVisitor();

    private LockWatchReferences() {
        // no
    }

    public static LockWatchReference entireTable(String qualifiedTableRef) {
        return ImmutableEntireTable.of(qualifiedTableRef);
    }

    public static LockWatchReference rowPrefix(String qualifiedTableRef, byte[] rowPrefix) {
        return ImmutableRowPrefix.of(qualifiedTableRef, rowPrefix);
    }

    public static LockWatchReference rowRange(String qualifiedTableRef, byte[] startInclusive, byte[] endExclusive) {
        return ImmutableRowRange.of(qualifiedTableRef, startInclusive, endExclusive);
    }

    public static LockWatchReference exactRow(String qualifiedTableRef, byte[] row) {
        return ImmutableExactRow.of(qualifiedTableRef, row);
    }

    public static LockWatchReference exactCell(String qualifiedTableRef, byte[] row, byte[] col) {
        return ImmutableExactCell.of(qualifiedTableRef, row, col);
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = ImmutableEntireTable.class, name = EntireTable.TYPE),
        @JsonSubTypes.Type(value = ImmutableRowPrefix.class, name = RowPrefix.TYPE),
        @JsonSubTypes.Type(value = ImmutableRowRange.class, name = RowRange.TYPE),
        @JsonSubTypes.Type(value = ImmutableExactRow.class, name = ExactRow.TYPE),
        @JsonSubTypes.Type(value = ImmutableExactCell.class, name = ExactCell.TYPE)
    })
    public interface LockWatchReference {
        <T> T accept(Visitor<T> visitor);
    }

    public interface Visitor<T> {
        T visit(EntireTable reference);

        T visit(RowPrefix reference);

        T visit(RowRange reference);

        T visit(ExactRow reference);

        T visit(ExactCell reference);
    }

    @Value.Immutable
    @PackageVisibleImmutablesStyle
    @JsonDeserialize(as = ImmutableEntireTable.class)
    @JsonSerialize(as = ImmutableEntireTable.class)
    @JsonTypeName(EntireTable.TYPE)
    abstract static class EntireTable implements LockWatchReference {
        static final String TYPE = "fullTable";

        @Value.Parameter
        abstract String qualifiedTableRef();

        @Override
        public <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    @Value.Immutable
    @PackageVisibleImmutablesStyle
    @JsonDeserialize(as = ImmutableRowPrefix.class)
    @JsonSerialize(as = ImmutableRowPrefix.class)
    @JsonTypeName(RowPrefix.TYPE)
    abstract static class RowPrefix implements LockWatchReference {
        static final String TYPE = "rowPrefix";

        @Value.Parameter
        abstract String qualifiedTableRef();

        @Value.Parameter
        abstract byte[] row();

        @Override
        public <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    @Value.Immutable
    @PackageVisibleImmutablesStyle
    @JsonDeserialize(as = ImmutableRowRange.class)
    @JsonSerialize(as = ImmutableRowRange.class)
    @JsonTypeName(RowRange.TYPE)
    abstract static class RowRange implements LockWatchReference {
        static final String TYPE = "rowRange";

        @Value.Parameter
        abstract String qualifiedTableRef();

        @Value.Parameter
        abstract byte[] startInclusive();

        @Value.Parameter
        abstract byte[] endExclusive();

        @Override
        public <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    @Value.Immutable
    @PackageVisibleImmutablesStyle
    @JsonDeserialize(as = ImmutableExactRow.class)
    @JsonSerialize(as = ImmutableExactRow.class)
    @JsonTypeName(ExactRow.TYPE)
    abstract static class ExactRow implements LockWatchReference {
        static final String TYPE = "exactRow";

        @Value.Parameter
        abstract String qualifiedTableRef();

        @Value.Parameter
        abstract byte[] row();

        @Override
        public <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    @Value.Immutable
    @PackageVisibleImmutablesStyle
    @JsonDeserialize(as = ImmutableExactCell.class)
    @JsonSerialize(as = ImmutableExactCell.class)
    @JsonTypeName(ExactCell.TYPE)
    abstract static class ExactCell implements LockWatchReference {
        static final String TYPE = "exactCell";

        @Value.Parameter
        abstract String qualifiedTableRef();

        @Value.Parameter
        abstract byte[] row();

        @Value.Parameter
        abstract byte[] col();

        @Override
        public <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    private static final class LockDescriptorRangeVisitor implements Visitor<Range<LockDescriptor>> {
        @Override
        public Range<LockDescriptor> visit(EntireTable reference) {
            return AtlasLockDescriptorRanges.fullTable(reference.qualifiedTableRef());
        }

        @Override
        public Range<LockDescriptor> visit(RowPrefix reference) {
            return AtlasLockDescriptorRanges.rowPrefix(reference.qualifiedTableRef(), reference.row());
        }

        @Override
        public Range<LockDescriptor> visit(RowRange reference) {
            return AtlasLockDescriptorRanges.rowRange(
                    reference.qualifiedTableRef(), reference.startInclusive(), reference.endExclusive());
        }

        @Override
        public Range<LockDescriptor> visit(ExactRow reference) {
            return AtlasLockDescriptorRanges.exactRow(reference.qualifiedTableRef(), reference.row());
        }

        @Override
        public Range<LockDescriptor> visit(ExactCell reference) {
            return AtlasLockDescriptorRanges.exactCell(reference.qualifiedTableRef(), reference.row(), reference.col());
        }
    }
}
