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
import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
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
import org.immutables.value.Value;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MarkAndCasConsensusForgettingStore implements ConsensusForgettingStore {
    private final byte[] inProgressMarker;
    private final KeyValueService kvs;
    private final TableReference tableRef;
    private final ReadableConsensusForgettingStore reader;
    private final DisruptorAutobatcher<CasRequest, Void> autobatcher;

    public MarkAndCasConsensusForgettingStore(
            byte[] inProgressMarker,
            KeyValueService kvs,
            TableReference tableRef) {
        Preconditions.checkArgument(!kvs.getCheckAndSetCompatibility().consistentOnFailure());
        this.inProgressMarker = inProgressMarker;
        this.kvs = kvs;
        this.tableRef = tableRef;
        this.reader = new ReadableConsensusForgettingStoreImpl(kvs, tableRef);
        this.autobatcher = Autobatchers.coalescing(new CasCoalescingFunction())
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
        autobatcher.apply(ImmutableCasRequest.of(cell, inProgressMarker, value));
    }

    @Override
    public void atomicUpdate(Map<Cell, byte[]> values) throws MultiCheckAndSetException {
        values.forEach(this::atomicUpdate);
    }

    @Override
    public void checkAndTouch(Cell cell, byte[] value) throws CheckAndSetException {
        autobatcher.apply(ImmutableCasRequest.of(cell, value, value));
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


    class CasCoalescingFunction implements CoalescingRequestFunction<CasRequest, Void> {
        @Override
        public Map<CasRequest, Void> apply(Set<CasRequest> batch) {
            // filter out updates for same cell
            List<CasRequest> pendingUpdates = getFilteredUpdates(batch);
            Map<ByteBuffer, List<CasRequest>> pendingRawRequests = pendingUpdates.stream()
                    .collect(Collectors.groupingBy(
                            elem -> ByteBuffer.wrap(elem.cell().getRowName())));

            ImmutableMap.Builder<CasRequest, Void> resultBuilder = ImmutableMap.builder();
            for (Map.Entry<ByteBuffer, List<CasRequest>> requestEntry : pendingRawRequests.entrySet()) {
                ByteBuffer rowName = requestEntry.getKey();
                List<CasRequest> pendingRequests = requestEntry.getValue();
                MultiCheckAndSetRequest multiCheckAndSetRequest = multiCASRequest(tableRef, rowName, pendingRequests);
                try {
                    kvs.multiCheckAndSet(multiCheckAndSetRequest);
                    pendingRequests.forEach(req -> resultBuilder.put(req, null));
                } catch (Exception e) {
                    // todo(snanda)
                }
            }

            return resultBuilder.build();
        }

        private MultiCheckAndSetRequest multiCASRequest(
                TableReference tableRef, ByteBuffer rowName, List<CasRequest> requests) {
            Map<Cell, byte[]> expected = getValueMap(requests, CasRequest::expected);
            Map<Cell, byte[]> updates = getValueMap(requests, CasRequest::update);
            return MultiCheckAndSetRequest.multipleCells(tableRef, rowName.array(), expected, updates);
        }

        private Map<Cell, byte[]> getValueMap(
                List<CasRequest> requests, Function<CasRequest, ByteBuffer> valueExtractor) {
            return KeyedStream.of(requests)
                    .mapKeys(CasRequest::cell)
                    .map(valueExtractor::apply)
                    .map(ByteBuffer::array)
                    .collectToMap();
        }

        private List<CasRequest> getFilteredUpdates(Set<CasRequest> batch) {
            Map<Cell, List<CasRequest>> partitionedElems = batch.stream().collect(Collectors.groupingBy(CasRequest::cell));

            ImmutableList.Builder<CasRequest> pendingUpdates = ImmutableList.builder();

            for (Map.Entry<Cell, List<CasRequest>> entry : partitionedElems.entrySet()) {
                List<CasRequest> requestedUpdates = entry.getValue();

                Collections.sort(requestedUpdates, Comparator.comparingInt(this::rank));
                pendingUpdates.add(requestedUpdates.remove(0));
            }

            return pendingUpdates.build();
        }

        // todo(snanda): idea is touch > state change to commit > state change to abort and ignore all other requests
        private int rank(CasRequest req) {
            if (req.expected().equals(req.update())) {
                return 1;
            }
            if (req.expected().equals(ByteBuffer.wrap(TransactionConstants.TICKETS_ENCODING_ABORTED_TRANSACTION_VALUE))) {
                return 3;
            }
            return 2;
        }
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
}
