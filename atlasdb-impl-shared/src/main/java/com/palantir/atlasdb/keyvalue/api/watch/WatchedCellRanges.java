/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api.watch;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.RowReference;
import com.palantir.dialogue.DialogueImmutablesStyle;
import org.immutables.value.Value;

public final class WatchedCellRanges {
    private WatchedCellRanges() {
        // utility
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = ImmutableWatchedTableReference.class, name = WatchedTableReference.TYPE),
        @JsonSubTypes.Type(value = ImmutableWatchedRowReference.class, name = WatchedRowReference.TYPE)
    })
    public interface WatchedCellRange {
        // TODO(gs): usages of these should probably be replaced with accepting a visitor.
        TableReference tableReference();

        <T> T accept(Visitor<T> visitor);
    }

    public interface Visitor<T> {
        T visit(WatchedTableReference tableReference);

        T visit(WatchedRowReference rowReference);
    }

    @Value.Immutable
    @DialogueImmutablesStyle
    @JsonTypeName(WatchedTableReference.TYPE)
    public abstract static class WatchedTableReference implements WatchedCellRange {
        static final String TYPE = "fullTable";

        @Value.Parameter
        @Override
        public abstract TableReference tableReference();

        @Override
        public <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }

        public static WatchedTableReference of(TableReference tableReference) {
            return ImmutableWatchedTableReference.of(tableReference);
        }
    }

    @Value.Immutable
    @DialogueImmutablesStyle
    @JsonTypeName(WatchedRowReference.TYPE)
    public abstract static class WatchedRowReference implements WatchedCellRange {
        static final String TYPE = "exactRow";

        @Value.Parameter
        public abstract RowReference rowReference();

        @Value.Derived
        @Override
        public TableReference tableReference() {
            return rowReference().tableRef();
        }

        @Override
        public <T> T accept(Visitor<T> visitor) {
            return visitor.visit(this);
        }

        public static WatchedRowReference of(RowReference rowReference) {
            return ImmutableWatchedRowReference.of(rowReference);
        }
    }
}
