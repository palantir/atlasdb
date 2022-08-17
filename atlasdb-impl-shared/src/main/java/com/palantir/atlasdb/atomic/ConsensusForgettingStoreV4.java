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

package com.palantir.atlasdb.atomic;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.MultiCheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.MultiCheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.logsafe.Preconditions;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ConsensusForgettingStoreV4 implements ConsensusForgettingStore {
    private final byte[] inProgressMarker;
    private final KeyValueService kvs;
    private final TableReference tableRef;
    private final ConsensusForgettingStoreReader reader;

    public ConsensusForgettingStoreV4(byte[] inProgressMarker, KeyValueService kvs, TableReference tableRef) {
        Preconditions.checkArgument(!kvs.getCheckAndSetCompatibility().consistentOnFailure());
        this.inProgressMarker = inProgressMarker;
        this.kvs = kvs;
        this.tableRef = tableRef;
        this.reader = new ConsensusForgettingStoreReaderImpl(kvs, tableRef);
    }

    @Override
    public void mark(Cell cell) {
        mark(ImmutableSet.of(cell));
    }

    @Override
    public void mark(Set<Cell> cells) {
        kvs.put(tableRef, cells.stream().collect(Collectors.toMap(x -> x, _ignore -> inProgressMarker)), 0L);
    }

    /**
     * Atomically updates cells that have been marked. Throws {@code CheckAndSetException} if cell to update has not
     * been marked.
     */
    @Override
    public void atomicUpdate(Cell cell, byte[] value) throws CheckAndSetException {
        CheckAndSetRequest request = CheckAndSetRequest.singleCell(tableRef, cell, inProgressMarker, value);
        kvs.checkAndSet(request);
    }

    @Override
    public void atomicUpdate(Map<Cell, byte[]> values) throws MultiCheckAndSetException {
        byte[] row = getRowName(values);
        Map<Cell, byte[]> expected =
                values.keySet().stream().collect(Collectors.toMap(cell -> cell, _ignore -> inProgressMarker));
        MultiCheckAndSetRequest request = MultiCheckAndSetRequest.multipleCells(tableRef, row, expected, values);
        kvs.multiCheckAndSet(request);
    }

    @Override
    public void checkAndTouch(Cell cell, byte[] value) throws CheckAndSetException {
        CheckAndSetRequest request = CheckAndSetRequest.singleCell(tableRef, cell, value, value);
        kvs.checkAndSet(request);
    }

    @Override
    public void checkAndTouch(Map<Cell, byte[]> values) throws MultiCheckAndSetException {
        byte[] row = getRowName(values);
        MultiCheckAndSetRequest request = MultiCheckAndSetRequest.multipleCells(tableRef, row, values, values);
        kvs.multiCheckAndSet(request);
    }

    @Override
    public ListenableFuture<Optional<byte[]>> get(Cell cell) {
        return reader.get(cell);
    }

    @Override
    public ListenableFuture<Map<Cell, byte[]>> getMultiple(Iterable<Cell> cells) {
        return reader.getMultiple(cells);
    }

    @Override
    public void put(Cell cell, byte[] value) {
        put(ImmutableMap.of(cell, value));
    }

    @Override
    public void put(Map<Cell, byte[]> values) {
        kvs.setOnce(tableRef, values);
    }

    private byte[] getRowName(Map<Cell, byte[]> values) {
        Set<ByteBuffer> rows = values.keySet().stream()
                .map(Cell::getRowName)
                .map(ByteBuffer::wrap)
                .collect(Collectors.toSet());
        Preconditions.checkState(rows.size() == 1, "Only allowed to make batch cells across one row.");
        return Iterables.getOnlyElement(rows).array();
    }
}
