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

import java.util.Optional;

import com.google.common.collect.Ordering;
import com.palantir.atlasdb.v2.api.api.NewIds.Cell;
import com.palantir.atlasdb.v2.api.api.NewIds.StoredValue;
import com.palantir.atlasdb.v2.api.api.NewValue;
import com.palantir.atlasdb.v2.api.api.ScanDefinition;

import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import io.vavr.collection.TreeMap;

public final class TableReads {
    static final TableReads EMPTY = new TableReads(HashMap.empty(), HashMap.empty());

    private final Map<Cell, NewValue> reads;
    private final Map<ScanDefinition, EarlyScanTermination> scanEnds;

    private TableReads(Map<Cell, NewValue> reads, Map<ScanDefinition, EarlyScanTermination> scanEnds) {
        this.reads = reads;
        this.scanEnds = scanEnds;
    }

    public Optional<StoredValue> get(Cell cell) {
        return reads.get(cell).toJavaOptional().flatMap(NewValue::maybeData);
    }

    public ScanDefinition toConflictCheckingScan() {
        throw new UnsupportedOperationException();
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    private static final class EarlyScanTermination {
        private static final EarlyScanTermination REACHED_END = new EarlyScanTermination(null);

        private final NewValue value;

        private EarlyScanTermination(NewValue value) {
            this.value = value;
        }

        private boolean readUntilEnd() {
            return value == null;
        }

        private NewValue value() {
            return checkNotNull(value);
        }

        private EarlyScanTermination merge(EarlyScanTermination other, Ordering<NewValue> comparator) {
            if (readUntilEnd()) {
                return this;
            } else if (other.readUntilEnd()) {
                return other;
            }
            return comparator.onResultOf(EarlyScanTermination::value).max(this, other);
        }
    }

    public static final class Builder {
        private Map<Cell, NewValue> reads;
        // TODO use a type that does not contain the table, or validates the table...
        private Map<ScanDefinition, EarlyScanTermination> scanEnds;

        public Builder() {
            reads = TreeMap.empty();
            scanEnds = HashMap.empty();
        }

        public Builder(TableReads tableReads) {
            this.reads = tableReads.reads;
            this.scanEnds = tableReads.scanEnds;
        }

        public Builder reachedEnd(ScanDefinition scan) {
            scanEnds = scanEnds.put(scan, EarlyScanTermination.REACHED_END);
            return this;
        }

        public Builder putScanEnd(ScanDefinition scan, NewValue end) {
            Map<ScanDefinition, EarlyScanTermination> map = HashMap.of(scan, new EarlyScanTermination(end));
            Ordering<NewValue> comparator = scan.attributes().cellComparator();
            // very likely need to optimize this... initial implementation seems wildly inefficient
            reads = reads.put(end.cell(), end);
            scanEnds = scanEnds.merge(map, (oldEnd, newEnd) -> oldEnd.merge(newEnd, comparator));
            return this;
        }

        public Builder clear() {
            reads = TreeMap.empty();
            return this;
        }

        public Builder mergeFrom(TableReads other) {
            if (reads.isEmpty() && scanEnds.isEmpty()) {
                reads = other.reads;
                scanEnds = other.scanEnds;
                return this;
            }
            for (NewValue value : other.reads.values()) {
                reads = reads.put(value.cell(), value);
            }
            other.scanEnds.forEach((scan, end) -> scanEnds = scanEnds.put(scan, end));
            return this;
        }

        public TableReads build() {
            return new TableReads(reads, scanEnds);
        }

        public TableReads buildPartial() {
            return build();
        }
    }
}
