/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.v2.api.transaction.state;

import com.palantir.atlasdb.v2.api.NewIds;
import com.palantir.atlasdb.v2.api.NewValue;

import io.vavr.collection.SortedMap;
import io.vavr.collection.TreeMap;

public final class TableWrites {
    static final TableWrites EMPTY = new TableWrites(TreeMap.empty());

    private final SortedMap<NewIds.Cell, NewValue.TransactionValue> writes;

    private TableWrites(SortedMap<NewIds.Cell, NewValue.TransactionValue> writes) {
        this.writes = writes;
    }

    boolean isEmpty() {
        return writes.isEmpty();
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    public static final class Builder {
        private SortedMap<NewIds.Cell, NewValue.TransactionValue> writes;

        public Builder() {
            writes = TreeMap.empty();
        }

        public Builder(TableWrites tableWrites) {
            this.writes = tableWrites.writes;
        }

        public Builder put(NewValue.TransactionValue value) {
            writes = writes.put(value.cell(), value);
            return this;
        }

        public Builder clear() {
            writes = TreeMap.empty();
            return this;
        }

        public Builder mergeFrom(TableWrites other) {
            if (writes.isEmpty()) {
                writes = other.writes;
                return this;
            }
            for (NewValue.TransactionValue value : other.writes.values()) {
                writes = writes.put(value.cell(), value);
            }
            return this;
        }

        public TableWrites build() {
            return new TableWrites(writes);
        }

        public TableWrites buildPartial() {
            return build();
        }
    }
}
