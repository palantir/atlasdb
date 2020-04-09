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

package com.palantir.atlasdb.v2.api.transaction.scanner;

import java.util.Comparator;
import java.util.Set;

import com.palantir.atlasdb.v2.api.api.AsyncIterator;
import com.palantir.atlasdb.v2.api.api.NewIds.Cell;
import com.palantir.atlasdb.v2.api.api.NewValue;
import com.palantir.atlasdb.v2.api.api.ScanDefinition;
import com.palantir.atlasdb.v2.api.api.ScanFilter;
import com.palantir.atlasdb.v2.api.iterators.AsyncIterators;
import com.palantir.atlasdb.v2.api.transaction.state.TransactionState;

// This can be optimized by pushing it into the KVS layer in some cases.
public final class StopAfterMarkerReader<T extends NewValue> implements Reader<T> {
    private final AsyncIterators iterators;
    private final Reader<T> delegate;

    public StopAfterMarkerReader(AsyncIterators iterators, Reader<T> delegate) {
        this.iterators = iterators;
        this.delegate = delegate;
    }

    @Override
    public AsyncIterator<T> scan(TransactionState state, ScanDefinition definition) {
        AsyncIterator<T> scan = delegate.scan(state, definition);
        Comparator<Cell> comparator = definition.filter().toComparator(definition.attributes());
        return definition.filter().accept(new ScanFilter.Visitor<AsyncIterator<T>>() {
            @Override
            public AsyncIterator<T> rowsAndColumns(ScanFilter.RowsFilter rows, ScanFilter.ColumnsFilter columns,
                    int limit) {
                return scan;
            }

            @Override
            public AsyncIterator<T> cells(Set<Cell> cells) {
                return scan;
            }

            @Override
            public AsyncIterator<T> withStoppingPoint(ScanFilter inner, Cell lastCellInclusive) {
                // this is inefficient, but correct.
                return iterators.filter(scan, element -> comparator.compare(element.cell(), lastCellInclusive) <= 0);
            }
        });
    }
}
