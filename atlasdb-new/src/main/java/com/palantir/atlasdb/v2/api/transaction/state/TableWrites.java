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

import static com.palantir.logsafe.Preconditions.checkNotNull;

import com.palantir.atlasdb.v2.api.NewIds.Cell;
import com.palantir.atlasdb.v2.api.NewIds.Table;
import com.palantir.atlasdb.v2.api.NewValue;
import com.palantir.atlasdb.v2.api.ScanDefinition;

import io.vavr.collection.Map;
import io.vavr.collection.SortedMap;
import io.vavr.collection.TreeMap;

public final class TableWrites {
    private final Table table;
    private final SortedMap<Cell, NewValue.TransactionValue> writes;

    private TableWrites(Table table,
            SortedMap<Cell, NewValue.TransactionValue> writes) {
        this.table = table;
        this.writes = writes;
    }

    public Table table() {
        return table;
    }

    public Map<Cell, NewValue.TransactionValue> data() {
        return writes;
    }

    public boolean containsCell(Cell cell) {
        return writes.containsKey(cell);
    }

    public ScanDefinition toConflictCheckingScan() {
        throw new UnsupportedOperationException();
    }

    boolean isEmpty() {
        return writes.isEmpty();
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    public static final class Builder {
        private Table table;
        private SortedMap<Cell, NewValue.TransactionValue> writes;

        public Builder() {
            writes = TreeMap.empty();
            table = null;
        }

        public Builder(TableWrites tableWrites) {
            this.writes = tableWrites.writes;
            this.table = tableWrites.table;
        }

        public Builder table(Table table) {
            this.table = table;
            return this;
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
            return new TableWrites(checkNotNull(table), writes);
        }

        public TableWrites buildPartial() {
            return build();
        }
    }
}
