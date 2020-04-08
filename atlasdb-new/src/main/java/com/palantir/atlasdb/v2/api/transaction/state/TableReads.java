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

import java.util.Optional;

import com.google.common.collect.Ordering;
import com.palantir.atlasdb.v2.api.NewIds;
import com.palantir.atlasdb.v2.api.NewIds.Cell;
import com.palantir.atlasdb.v2.api.NewValue;
import com.palantir.atlasdb.v2.api.NewValue.CommittedValue;
import com.palantir.atlasdb.v2.api.ScanDefinition;

import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import io.vavr.collection.TreeMap;

public final class TableReads {
    static final TableReads EMPTY = new TableReads(HashMap.empty());

    private final Map<Cell, CommittedValue> reads;

    private TableReads(Map<Cell, CommittedValue> reads) {
        this.reads = reads;
    }

    boolean isEmpty() {
        return reads.isEmpty();
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    public static final class Builder {
        private Map<Cell, CommittedValue> reads;
        private Map<ScanDefinition, Optional<CommittedValue>> scanEnds;

        public Builder() {
            reads = TreeMap.empty();
        }

        public Builder(TableReads tableReads) {
            this.reads = tableReads.reads;
        }

        public Builder put(NewValue value) {
            return value.accept(CommittedValueExtractor.INSTANCE).map(this::put).orElse(this);
        }

        public ScanDefinition putScanEnd(ScanDefinition scan, CommittedValue end) {
            Ordering<NewValue> comparator = scan.attributes().cellComparator();
            if (!scanEnds.containsKey(scan)) {
            }
            Optional<CommittedValue> present = scanEnds.get(scan).getOrElse(null);
            scanEnds.put()
        }

        public Builder put(CommittedValue value) {
            reads = reads.put(value.cell(), value);
            return this;
        }

        public Builder clear() {
            reads = TreeMap.empty();
            return this;
        }

        public Builder mergeFrom(TableReads other) {
            if (reads.isEmpty()) {
                reads = other.reads;
                return this;
            }
            for (CommittedValue value : other.reads.values()) {
                reads = reads.put(value.cell(), value);
            }
            return this;
        }

        public TableReads build() {
            return new TableReads(reads);
        }

        public TableReads buildPartial() {
            return build();
        }
    }

    private enum CommittedValueExtractor implements NewValue.Visitor<Optional<NewValue>> {
        INSTANCE;

        @Override
        public Optional<NewValue> kvsValue(Cell cell, long startTimestamp, NewIds.StoredValue data) {
            return Optional.empty();
        }

        @Override
        public Optional<NewValue> abortedValue(Cell cell, long startTimestamp) {
            return Optional.empty();
        }

        @Override
        public Optional<NewValue> notYetCommittedValue(Cell cell, long startTimestamp, NewIds.StoredValue data) {
            return Optional.empty();
        }

        @Override
        public Optional<NewValue> committedValue(Cell cell, long commitTimestamp, NewIds.StoredValue data) {
            return Optional.of(NewValue.committedValue(cell, commitTimestamp, data));
        }

        @Override
        public Optional<NewValue> transactionValue(Cell cell, Optional<NewIds.StoredValue> maybeData) {
            return Optional.empty();
        }
    }
}
