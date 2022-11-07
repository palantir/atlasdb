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

package com.palantir.atlasdb.atomic.mcas;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.atomic.AtomicUpdateResult;
import com.palantir.atlasdb.atomic.ConsensusForgettingStore;
import com.palantir.atlasdb.atomic.ImmutableAtomicUpdateResult;
import com.palantir.atlasdb.atomic.ReadableConsensusForgettingStore;
import com.palantir.atlasdb.atomic.ReadableConsensusForgettingStoreImpl;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.MultiCheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.encoding.TwoPhaseEncodingStrategy;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class MarkAndCasConsensusForgettingStore implements ConsensusForgettingStore {
    public static final ByteBuffer WRAPPED_ABORTED_TRANSACTION_STAGING_VALUE =
            ByteBuffer.wrap(TwoPhaseEncodingStrategy.ABORTED_TRANSACTION_STAGING_VALUE);

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
        this.autobatcher = Autobatchers.<CasRequest, Void>independent(list -> processBatch(kvs, tableRef, list))
                .safeLoggablePurpose("mcas-batching-store")
                .batchFunctionTimeout(Duration.ofMinutes(2))
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
     * Atomically updates cells that have been marked. The MCAS calls to KVS are batched.
     *
     * @return {@link AtomicUpdateResult} with success if the atomic update was successful. Else,
     * {@link AtomicUpdateResult} with {@link KeyAlreadyExistsException} with detail if there is a value
     * present against this key.
     */
    @Override
    public AtomicUpdateResult atomicUpdate(Cell cell, byte[] value) {
        ImmutableCasRequest casRequest = ImmutableCasRequest.of(cell, inProgressMarkerBuffer, ByteBuffer.wrap(value));
        return getResultForFuture(casRequest);
    }

    /**
     * This call serially delegated to {@link MarkAndCasConsensusForgettingStore#atomicUpdate(Cell, byte[])}
     * and does not guarantee atomicity across cells.
     * The MCAS calls to KVS are batched and hence, in practice, it is possible that the group of cells is served
     * atomically.
     */
    @Override
    public AtomicUpdateResult atomicUpdate(Map<Cell, byte[]> values) {
        ImmutableList.Builder<Cell> committedKeys = ImmutableList.builder();
        ImmutableList.Builder<Cell> existingKeys = ImmutableList.builder();

        values.entrySet().stream()
                .map(entry -> atomicUpdate(entry.getKey(), entry.getValue()))
                .forEach(updateResult -> {
                    committedKeys.addAll(updateResult.knownSuccessfullyCommittedKeys());
                    existingKeys.addAll(updateResult.existingKeys());
                });

        return AtomicUpdateResult.create(committedKeys.build(), existingKeys.build());
    }

    @Override
    public void checkAndTouch(Cell cell, byte[] value) throws CheckAndSetException {
        ByteBuffer buffer = ByteBuffer.wrap(value);
        autobatcher.apply(ImmutableCasRequest.of(cell, buffer, buffer));
    }

    @Override
    public void checkAndTouch(Map<Cell, byte[]> values) throws CheckAndSetException {
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

    @SuppressWarnings("ByteBufferBackingArray")
    @VisibleForTesting
    static void processBatch(KeyValueService kvs, TableReference tableRef, List<BatchElement<CasRequest, Void>> batch) {
        List<BatchElement<CasRequest, Void>> pendingUpdates = filterOptimalUpdatePerCell(batch);

        List<CasRequestBatch> pendingBatchedRequests = KeyedStream.stream(pendingUpdates.stream()
                        .collect(Collectors.groupingBy(
                                elem -> ByteBuffer.wrap(elem.argument().cell().getRowName()))))
                .map((row, requests) -> new CasRequestBatch(tableRef, row.array(), requests))
                .values()
                .collect(Collectors.toList());

        // at this point, results for requests that will never be retried are already populated.
        // There is one request per row
        while (!pendingBatchedRequests.isEmpty()) {
            pendingBatchedRequests = pendingBatchedRequests.stream()
                    .filter(req -> !serveMcasRequest(kvs, req))
                    .collect(Collectors.toList());
        }
    }

    @VisibleForTesting
    static boolean serveMcasRequest(KeyValueService kvs, CasRequestBatch casRequestBatch) {
        try {
            kvs.multiCheckAndSet(casRequestBatch.getMcasRequest());
            // The above operation is atomic
            casRequestBatch.setSuccessForAllRequests();
        } catch (MultiCheckAndSetException ex) {
            casRequestBatch.processBatchWithException(MarkAndCasConsensusForgettingStore::shouldRetry, ex);
        }
        return casRequestBatch.isBatchServed();
    }

    // we only want to retry the requests where the actual matches the expected.

    @VisibleForTesting
    static boolean shouldRetry(BatchElement<CasRequest, Void> req, MultiCheckAndSetException ex) {
        CasRequest casRequest = req.argument();
        Cell cell = casRequest.cell();

        if (!ex.getActualValues().containsKey(cell)) {
            return false;
        }

        return casRequest.expected().equals(ByteBuffer.wrap(ex.getActualValues().get(cell)));
    }

    private AtomicUpdateResult getResultForFuture(CasRequest casRequest) {
        try {
            autobatcher.apply(casRequest).get();
            return ImmutableAtomicUpdateResult.builder()
                    .addKnownSuccessfullyCommittedKeys(casRequest.cell())
                    .build();
        } catch (Exception ex) {
            if (ex.getCause() instanceof KeyAlreadyExistsException) {
                return ImmutableAtomicUpdateResult.builder()
                        .addExistingKeys(casRequest.cell())
                        .build();
            }
            throw new SafeRuntimeException("Could not execute atomic update", ex);
        }
    }

    // Every request with a lower rank will never be tried. Requests for touch will fail with
    // `CheckAndSetException` and those for atomic updates will fail with `KeyAlreadyExistsException`.
    private static List<BatchElement<CasRequest, Void>> filterOptimalUpdatePerCell(
            List<BatchElement<CasRequest, Void>> batch) {

        Map<Cell, List<BatchElement<CasRequest, Void>>> partitionedElems = batch.stream()
                .collect(Collectors.groupingBy(elem -> elem.argument().cell()));

        ImmutableList.Builder<BatchElement<CasRequest, Void>> requestsToProcess = ImmutableList.builder();

        for (Map.Entry<Cell, List<BatchElement<CasRequest, Void>>> requestedUpdatesForCell :
                partitionedElems.entrySet()) {
            List<BatchElement<CasRequest, Void>> sortedPendingRequests = requestedUpdatesForCell.getValue().stream()
                    .sorted(Comparator.comparing(elem -> elem.argument().rank()))
                    .collect(Collectors.toList());
            if (!sortedPendingRequests.isEmpty()) {
                requestsToProcess.add(sortedPendingRequests.get(0));

                // we want to fail the requests that will never be tried eagerly.
                BatchElement<CasRequest, Void> elem;
                for (int i = 1; i < sortedPendingRequests.size(); i++) {
                    elem = sortedPendingRequests.get(i);
                    elem.result().setException(CasRequest.failureUntried(elem.argument()));
                }
            }
        }

        return requestsToProcess.build();
    }
}
