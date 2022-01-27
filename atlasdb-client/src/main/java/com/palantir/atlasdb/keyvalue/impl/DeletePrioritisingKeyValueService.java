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

package com.palantir.atlasdb.keyvalue.impl;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import com.palantir.atlasdb.keyvalue.api.AutoDelegate_KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.streams.KeyedStream;
import java.util.Map;

public class DeletePrioritisingKeyValueService implements AutoDelegate_KeyValueService {
    private final KeyValueService delegate;
    private final Multimap<CellReference, Long> deletedCellsAndTimestamps = Multimaps.synchronizedMultimap(
            MultimapBuilder.hashKeys().hashSetValues().build());

    private volatile boolean prioritiseDeletes = false;

    public DeletePrioritisingKeyValueService(KeyValueService delegate) {
        this.delegate = delegate;
    }

    @Override
    public KeyValueService delegate() {
        return delegate;
    }

    public void setPrioritiseDeletes(boolean prioritiseDeletes) {
        this.prioritiseDeletes = prioritiseDeletes;
    }

    @Override
    public void delete(TableReference tableRef, Multimap<Cell, Long> keys) {
        if (prioritiseDeletes) {
            deletedCellsAndTimestamps.putAll(KeyedStream.stream(keys)
                    .mapKeys(cell -> CellReference.of(tableRef, cell))
                    .collectToSetMultimap());
        }
        delegate.delete(tableRef, keys);
    }

    @Override
    public void put(TableReference tableRef, Map<Cell, byte[]> values, long timestamp) {
        values.forEach((cell, value) -> {
            if (!prioritiseDeletes
                    || !deletedCellsAndTimestamps.containsEntry(CellReference.of(tableRef, cell), timestamp))
                delegate.put(tableRef, ImmutableMap.of(cell, value), timestamp);
        });
    }

    @Override
    public void multiPut(Map<TableReference, ? extends Map<Cell, byte[]>> valuesByTable, final long timestamp) {
        valuesByTable.forEach((tableRef, values) -> put(tableRef, values, timestamp));
    }
}
