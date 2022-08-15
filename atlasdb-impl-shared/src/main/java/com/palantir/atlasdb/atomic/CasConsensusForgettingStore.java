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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.MultiCheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.MultiCheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.Preconditions;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CasConsensusForgettingStore implements ConsensusForgettingStore {
    private final byte[] inProgressMarker;
    private final KeyValueService kvs;
    private final TableReference tableRef;

    public CasConsensusForgettingStore(byte[] inProgressMarker, KeyValueService kvs, TableReference tableRef) {
        Preconditions.checkArgument(!kvs.getCheckAndSetCompatibility().consistentOnFailure());
        this.inProgressMarker = inProgressMarker;
        this.kvs = kvs;
        this.tableRef = tableRef;
    }
    @Override
    public void markInProgress(Cell cell) {
        markInProgress(ImmutableSet.of(cell));
    }

    @Override
    public void markInProgress(Set<Cell> cells) {
        kvs.put(tableRef, cells.stream().collect(Collectors.toMap(x -> x, _ignore -> inProgressMarker)), 0L);
    }

    @Override
    public void atomicUpdate(Cell cell, byte[] value) throws KeyAlreadyExistsException {
        CheckAndSetRequest request = CheckAndSetRequest.singleCell(tableRef, cell, inProgressMarker, value);
        kvs.checkAndSet(request);
    }

    @Override
    public void atomicUpdate(Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        byte[] row = values.keySet().iterator().next().getRowName();
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
        byte[] row = values.keySet().iterator().next().getRowName();
        MultiCheckAndSetRequest request = MultiCheckAndSetRequest.multipleCells(tableRef, row, values, values);
        kvs.multiCheckAndSet(request);
    }

    @Override
    public ListenableFuture<Optional<byte[]>> get(Cell cell) {
        return Futures.transform(
                getMultiple(ImmutableList.of(cell)),
                result -> Optional.ofNullable(result.get(cell)),
                MoreExecutors.directExecutor());
    }

    @Override
    public ListenableFuture<Map<Cell, byte[]>> getMultiple(Iterable<Cell> cells) {
        return Futures.transform(
                kvs.getAsync(
                        tableRef,
                        StreamSupport.stream(cells.spliterator(), false)
                                .collect(Collectors.toMap(x -> x, _ignore -> Long.MAX_VALUE))),
                result -> KeyedStream.stream(result).map(Value::getContents).collectToMap(),
                MoreExecutors.directExecutor());
    }

    @Override
    public void put(Cell cell, byte[] value) {
        put(ImmutableMap.of(cell, value));
    }

    @Override
    public void put(Map<Cell, byte[]> values) {
        kvs.setOnce(tableRef, values);
    }
}