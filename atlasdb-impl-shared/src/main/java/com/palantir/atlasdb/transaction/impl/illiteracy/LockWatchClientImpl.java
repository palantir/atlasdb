/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.illiteracy;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.timelock.watch.ExplicitLockPredicate;
import com.palantir.atlasdb.timelock.watch.ImmutableWatchStateQuery;
import com.palantir.atlasdb.timelock.watch.LockPredicate;
import com.palantir.atlasdb.timelock.watch.LockWatchRpcClient;
import com.palantir.atlasdb.timelock.watch.WatchIdentifier;
import com.palantir.atlasdb.timelock.watch.WatchStateResponse;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.logsafe.Preconditions;

public class LockWatchClientImpl implements LockWatchClient {
    private final Set<RowReference> interestingRows;
    private final Map<RowReference, WatchIdentifier> watches;
    private final RowStateCache rowStateCache;

    private final KeyValueService kvs;
    private final LockWatchRpcClient lockWatchRpcClient;
    private final ConflictDetectionManager conflictDetectionManager;

    public LockWatchClientImpl(KeyValueService kvs, TransactionService ts, LockWatchRpcClient lockWatchRpcClient,
            ConflictDetectionManager conflictDetectionManager) {
        interestingRows = Sets.newConcurrentHashSet();
        watches = Maps.newConcurrentMap();
        rowStateCache = new RowStateCache(kvs, ts);

        this.kvs = kvs;
        this.lockWatchRpcClient = lockWatchRpcClient;
        this.conflictDetectionManager = conflictDetectionManager;
    }

    @Override
    public void enableWatchForRow(RowReference rowReference) {
        if (interestingRows.contains(rowReference)) {
            return;
        }
        // TODO (jkong): Autobatch
        TableReference tableReference = rowReference.tableReference();
        ConflictHandler conflictHandler = conflictDetectionManager.get(tableReference);
        Preconditions.checkState(conflictHandler.lockRowsForConflicts(),
                "For the table " + tableReference + " we don't lock rows so enabling watch for rows is kind of"
                        + " meaningless");
        interestingRows.add(rowReference);

        // So this table does work by locking rows for conflicts.
        WatchStateResponse response = lockWatchRpcClient.registerOrGetStates(ImmutableWatchStateQuery.builder()
                .addNewPredicates(ExplicitLockPredicate.of(
                        AtlasRowLockDescriptor.of(tableReference.getQualifiedName(), rowReference.row())))
                .build());

        // TODO (jkong): What if you don't get a response?
        watches.put(rowReference, Iterables.getOnlyElement(response.registerResponses()).identifier());
    }

    public Map<Cell, Value> getRows(TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnSelection columnSelection,
            long timestamp) {
        Map<Cell, Value> result = Maps.newHashMap();
        Map<RowReference, WatchIdentifier> identifierMap = Maps.newHashMap();
        Set<byte[]> skippedRows = Sets.newTreeSet(UnsignedBytes.lexicographicalComparator());
        for (byte[] row : rows) {
            RowReference reference = ImmutableRowReference.builder().tableReference(tableRef).row(row).build();
            if (interestingRows.contains(reference)) {
                WatchIdentifier identifier = watches.get(reference);
                if (identifier != null) {
                    identifierMap.put(reference, identifier);
                }
            }
        }

        WatchStateResponse response = lockWatchRpcClient.registerOrGetStates(
                ImmutableWatchStateQuery.builder().addAllKnownIdentifiers(identifierMap.values()).build());
        Set<WatchIdentifier> recognisedIdentifiers = response.getStateResponses().keySet();

        reregisterLostIdentifiers(identifierMap, recognisedIdentifiers);

        identifierMap = Maps.filterValues(identifierMap, recognisedIdentifiers::contains);
        for (Map.Entry<RowReference, WatchIdentifier> entry : identifierMap.entrySet()) {
            Optional<Map<Cell, Value>> cache =
                    rowStateCache.get(entry.getKey(),
                            WatchIdentifierAndState.of(entry.getValue(), response.getStateResponses().get(entry.getValue())),
                            timestamp);
            if (cache.isPresent()) {
                skippedRows.add(entry.getKey().row());
                Map<Cell, Value> relevantResults = KeyedStream.stream(cache.get())
                        .filterKeys(cell -> columnSelection.contains(cell.getColumnName()))
                        .collectToMap();
                result.putAll(relevantResults);
            }
        }

        // These rows we couldn't get from the cache
        Set<byte[]> rowsToQueryFor = StreamSupport.stream(rows.spliterator(), false)
                .filter(t -> !skippedRows.contains(t))
                .collect(Collectors.toCollection(() -> new TreeSet<>(UnsignedBytes.lexicographicalComparator())));
        if (!rowsToQueryFor.isEmpty()) {
            result.putAll(kvs.getRows(tableRef, rowsToQueryFor, columnSelection, timestamp));
        }
        return result;
    }

    private void reregisterLostIdentifiers(Map<RowReference, WatchIdentifier> identifierMap,
            Set<WatchIdentifier> recognisedIdentifiers) {
        // TODO (jkong): Make this async?
        Set<WatchIdentifier> needToReRegister = Sets.difference(
                ImmutableSet.copyOf(identifierMap.values()),
                recognisedIdentifiers);
        Set<RowReference> rowReferencesToReRegister = Sets.newHashSet();
        for (Map.Entry<RowReference, WatchIdentifier> entry : watches.entrySet()) {
            if (needToReRegister.contains(entry.getValue())) {
                rowReferencesToReRegister.add(entry.getKey());
            }
        }
        Map<LockPredicate, RowReference> preds = KeyedStream.of(rowReferencesToReRegister)
                .mapKeys(rr -> ExplicitLockPredicate.of(
                        AtlasRowLockDescriptor.of(rr.tableReference().getQualifiedName(), rr.row())))
                .collectToMap();

        WatchStateResponse watchStateResponse = lockWatchRpcClient.registerOrGetStates(
                ImmutableWatchStateQuery.builder()
                        .addAllNewPredicates(preds.keySet()).build());
        watchStateResponse.registerResponses().forEach(rwr ->
                watches.put(preds.get(rwr.predicate()), rwr.identifier()));
    }

    @Override
    public void enableWatchForCell(CellReference cellReference) {
        throw new UnsupportedOperationException("Not yet implemented! Sorry");
    }
}
