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
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.keyvalue.api.*;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.Preconditions;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.immutables.value.Value;

public class MarkAndCasConsensusForgettingStore implements ConsensusForgettingStore {
    private final byte[] inProgressMarker;
    private final KeyValueService kvs;
    private final TableReference tableRef;
    private final ReadableConsensusForgettingStore reader;
    private final DisruptorAutobatcher<CasRequest, Void> autobatcher;

    public MarkAndCasConsensusForgettingStore(
            byte[] inProgressMarker,
            KeyValueService kvs,
            TableReference tableRef,
            DisruptorAutobatcher<CasRequest, Void> autobatcher) {
        Preconditions.checkArgument(!kvs.getCheckAndSetCompatibility().consistentOnFailure());
        this.inProgressMarker = inProgressMarker;
        this.kvs = kvs;
        this.tableRef = tableRef;
        this.reader = new ReadableConsensusForgettingStoreImpl(kvs, tableRef);
        this.autobatcher = autobatcher;
    }

    public static MarkAndCasConsensusForgettingStore create(
            byte[] inProgressMarker, KeyValueService kvs, TableReference tableRef) {
        DisruptorAutobatcher<CasRequest, Void> autobatcher = Autobatchers.<CasRequest, Void>independent(
                        x -> processBatch(kvs, tableRef, x))
                .safeLoggablePurpose("mcas-batching-store")
                .batchFunctionTimeout(Duration.ofMinutes(5))
                .build();
        return new MarkAndCasConsensusForgettingStore(inProgressMarker, kvs, tableRef, autobatcher);
    }

    private static void processBatch(
            KeyValueService kvs, TableReference tableRef, List<BatchElement<CasRequest, Void>> batch) {
        // filter out updates for same cell
        List<BatchElement<CasRequest, Void>> pendingUpdates = getFilteredUpdates(batch);
        Map<ByteBuffer, List<BatchElement<CasRequest, Void>>> pendingRawRequests = pendingUpdates.stream()
                .collect(Collectors.groupingBy(
                        elem -> ByteBuffer.wrap(elem.argument().cell().getRowName())));
        for (Map.Entry<ByteBuffer, List<BatchElement<CasRequest, Void>>> requestEntry : pendingRawRequests.entrySet()) {
            ByteBuffer rowName = requestEntry.getKey();
            List<BatchElement<CasRequest, Void>> pendingRequests = requestEntry.getValue();
            MultiCheckAndSetRequest multiCheckAndSetRequest = multiCASRequest(tableRef, rowName, pendingRequests);
            try {
                kvs.multiCheckAndSet(multiCheckAndSetRequest);
                pendingRequests.forEach(req -> req.result().set(null));
            } catch (Exception e) {
                pendingRequests.forEach(req -> req.result().setException(e));
            }
        }
    }

    private static MultiCheckAndSetRequest multiCASRequest(
            TableReference tableRef, ByteBuffer rowName, List<BatchElement<CasRequest, Void>> requests) {
        Map<Cell, byte[]> expected = KeyedStream.of(requests)
                .mapKeys(elem -> elem.argument().cell())
                .map(elem -> elem.argument().expected())
                .collectToMap();
        Map<Cell, byte[]> updates = KeyedStream.of(requests)
                .mapKeys(elem -> elem.argument().cell())
                .map(elem -> elem.argument().update())
                .collectToMap();
        return MultiCheckAndSetRequest.multipleCells(tableRef, rowName.array(), expected, updates);
    }

    private static List<BatchElement<CasRequest, Void>> getFilteredUpdates(List<BatchElement<CasRequest, Void>> batch) {
        Map<Cell, List<BatchElement<CasRequest, Void>>> partitionedElems = batch.stream()
                .collect(Collectors.groupingBy(elem -> elem.argument().cell()));

        ImmutableList.Builder<BatchElement<CasRequest, Void>> pendingUpdates = ImmutableList.builder();

        for (Map.Entry<Cell, List<BatchElement<CasRequest, Void>>> entry : partitionedElems.entrySet()) {
            List<BatchElement<CasRequest, Void>> requestedUpdates = entry.getValue();

            // todo(snanda): think about the algo here and the exception handling
            pendingUpdates.add(requestedUpdates.remove(0));

            for (BatchElement<CasRequest, Void> reject : requestedUpdates) {
                reject.result().setException(new CheckAndSetException("Another check and set is in progress."));
            }
        }

        return pendingUpdates.build();
    }

    @Override
    public void mark(Cell cell) {
        mark(ImmutableSet.of(cell));
    }

    @Override
    public void mark(Set<Cell> cells) {
        // atomic updates generally happen at the Cassandra wall clock time of the operation, so we need the mark to
        // have a lower write time than what any atomic update might attempt to write
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
        //        autobatcher.apply()
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
        Preconditions.checkState(rows.size() == 1, "Only allowed to make batch updates to cells across one row.");
        return Iterables.getOnlyElement(rows).array();
    }

    @Value.Immutable
    interface CasRequest {
        @Value.Parameter
        Cell cell();

        @Value.Parameter
        byte[] expected();

        @Value.Parameter
        byte[] update();
    }
}
