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
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.MultiCheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.MultiCheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.Preconditions;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.immutables.value.Value;

public class MarkAndCasConsensusForgettingStore implements ConsensusForgettingStore {
    private final ByteBuffer inProgressMarkerBuffer;
    private final byte[] inProgressMarker;
    private final KeyValueService kvs;
    private final TableReference tableRef;
    private final ReadableConsensusForgettingStore reader;
    private final DisruptorAutobatcher<CasRequest, Void> autobatcher;

    public MarkAndCasConsensusForgettingStore(byte[] inProgressMarker, KeyValueService kvs, TableReference tableRef) {
        Preconditions.checkArgument(!kvs.getCheckAndSetCompatibility().consistentOnFailure());
        this.inProgressMarker = inProgressMarker;
        this.inProgressMarkerBuffer = ByteBuffer.wrap(inProgressMarker);
        this.kvs = kvs;
        this.tableRef = tableRef;
        this.reader = new ReadableConsensusForgettingStoreImpl(kvs, tableRef);
        this.autobatcher = Autobatchers.<CasRequest, Void>independent((list) -> processBatch(kvs, tableRef, list))
                .safeLoggablePurpose("mcas-batching-store")
                .batchFunctionTimeout(Duration.ofMinutes(5))
                .build();
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
        autobatcher.apply(ImmutableCasRequest.of(cell, inProgressMarkerBuffer, ByteBuffer.wrap(value)));
    }

    @Override
    public void atomicUpdate(Map<Cell, byte[]> values) throws MultiCheckAndSetException {
        values.forEach(this::atomicUpdate);
    }

    @Override
    public void checkAndTouch(Cell cell, byte[] value) throws CheckAndSetException {
        ByteBuffer buffer = ByteBuffer.wrap(value);
        autobatcher.apply(ImmutableCasRequest.of(cell, buffer, buffer));
    }

    @Override
    public void checkAndTouch(Map<Cell, byte[]> values) throws MultiCheckAndSetException {
        values.forEach(this::checkAndTouch);
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

    private static void processBatch(
            KeyValueService kvs, TableReference tableRef, List<BatchElement<CasRequest, Void>> batch) {
        List<BatchElement<CasRequest, Void>> pendingUpdates = getFilteredUpdates(batch);
        Map<ByteBuffer, List<BatchElement<CasRequest, Void>>> pendingRawRequests = pendingUpdates.stream()
                .collect(Collectors.groupingBy(
                        elem -> ByteBuffer.wrap(elem.argument().cell().getRowName())));

        // There is one request per row
        ImmutableMap.Builder<CasRequest, CasResponse> resultMap = ImmutableMap.builder();
        for (Map.Entry<ByteBuffer, List<BatchElement<CasRequest, Void>>> requestEntry : pendingRawRequests.entrySet()) {
            ByteBuffer rowName = requestEntry.getKey();
            List<BatchElement<CasRequest, Void>> pendingRequests = requestEntry.getValue();
            MultiCheckAndSetRequest multiCheckAndSetRequest = multiCASRequest(tableRef, rowName, pendingRequests);
            try {
                kvs.multiCheckAndSet(multiCheckAndSetRequest);
                pendingRequests.forEach(req -> resultMap.put(req.argument(), CasResponse.success()));
            } catch (Exception e) {
                pendingRequests.forEach(req -> resultMap.put(req.argument(), CasResponse.failure(e)));
            }
        }

        CasResponse defaultResponse =
                CasResponse.failure(new CheckAndSetException("Another check and set is in progress."));
        Map<CasRequest, CasResponse> results = resultMap.build();
        batch.forEach(elem -> {
            CasResponse response = results.getOrDefault(elem.argument(), defaultResponse);
            if (response.successful()) {
                elem.result().set(null);
            } else {
                elem.result().setException(response.exception().get());
            }
        });
    }

    private static MultiCheckAndSetRequest multiCASRequest(
            TableReference tableRef, ByteBuffer rowName, List<BatchElement<CasRequest, Void>> requests) {
        Map<Cell, byte[]> expected = getValueMap(requests, CasRequest::expected);
        Map<Cell, byte[]> updates = getValueMap(requests, CasRequest::update);
        return MultiCheckAndSetRequest.multipleCells(tableRef, rowName.array(), expected, updates);
    }

    private static Map<Cell, byte[]> getValueMap(
            List<BatchElement<CasRequest, Void>> requests, Function<CasRequest, ByteBuffer> valueExtractor) {
        return KeyedStream.of(requests)
                .mapKeys(elem -> elem.argument().cell())
                .map(elem -> valueExtractor.apply(elem.argument()).array())
                .collectToMap();
    }

    // everything with a different rank fails, and other than that, same rank elements are coalesced.
    private static List<BatchElement<CasRequest, Void>> getFilteredUpdates(List<BatchElement<CasRequest, Void>> batch) {

        Map<Cell, List<BatchElement<CasRequest, Void>>> partitionedElems = batch.stream()
                .collect(Collectors.groupingBy(elem -> elem.argument().cell()));

        ImmutableList.Builder<BatchElement<CasRequest, Void>> pendingUpdates = ImmutableList.builder();

        for (Map.Entry<Cell, List<BatchElement<CasRequest, Void>>> entry : partitionedElems.entrySet()) {
            List<BatchElement<CasRequest, Void>> requestedUpdates = entry.getValue();

            Collections.sort(requestedUpdates, Comparator.comparingInt(elem -> rank(elem.argument())));
            pendingUpdates.add(requestedUpdates.get(0));
        }

        return pendingUpdates.build();
    }

    // The idea is touch > state change to commit > state change to abort and ignore all the other requests.
    private static int rank(CasRequest req) {
        if (req.expected().equals(req.update())) {
            return 1;
        }
        if (req.expected().equals(ByteBuffer.wrap(TransactionConstants.TICKETS_ENCODING_ABORTED_TRANSACTION_VALUE))) {
            return 3;
        }
        return 2;
    }

    @Value.Immutable
    interface CasRequest {
        @Value.Parameter
        Cell cell();

        @Value.Parameter
        ByteBuffer expected();

        @Value.Parameter
        ByteBuffer update();
    }

    @Value.Immutable
    interface CasResponse {
        boolean successful();

        Optional<Exception> exception();

        // todo(snanda) add checks
        static CasResponse success() {
            return ImmutableCasResponse.builder().successful(true).build();
        }

        static CasResponse failure(Exception e) {
            return ImmutableCasResponse.builder().successful(false).exception(e).build();
        }
    }
}
