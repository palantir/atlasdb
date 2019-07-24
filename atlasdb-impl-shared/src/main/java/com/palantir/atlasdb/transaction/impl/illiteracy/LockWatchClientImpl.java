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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.Preconditions;

public class LockWatchClientImpl implements WatchRegistry {
    private final Set<RowReference> interestingRows;
    private final RowStateCache rowStateCache;

    private final KeyValueService kvs;
    private final RemoteLockWatchClient remoteLockWatchClient;
    private final ConflictDetectionManager conflictDetectionManager;

    public LockWatchClientImpl(KeyValueService kvs, TransactionService ts, RemoteLockWatchClient remoteLockWatchClient,
            ConflictDetectionManager conflictDetectionManager) {
        interestingRows = Sets.newConcurrentHashSet();
        rowStateCache = new RowStateCache(kvs, ts);

        this.kvs = kvs;
        this.remoteLockWatchClient = remoteLockWatchClient;
        this.conflictDetectionManager = conflictDetectionManager;
    }

    @Override
    public void enableWatchForRows(Set<RowReference> rowReferences) {
        for (RowReference rowReference : rowReferences) {
            if (interestingRows.contains(rowReference)) {
                return;
            }
            TableReference tableReference = rowReference.tableReference();
            ConflictHandler conflictHandler = conflictDetectionManager.get(tableReference);
            Preconditions.checkState(conflictHandler.lockRowsForConflicts(),
                    "For the table " + tableReference + " we don't lock rows so enabling watch for rows is kind of"
                            + " meaningless");
            interestingRows.add(rowReference);
        }
    }

    @Override
    public Set<RowReference> disableWatchForRows(Set<RowReference> rowReference) {
        Set<RowReference> removed = Sets.newHashSet();
        for (RowReference reference : rowReference) {
            boolean removedIndividual = interestingRows.remove(rowReference);
            if (removedIndividual) {
                removed.add(reference);
            }
        }
        return removed;
    }

    @Override
    public Set<RowReference> filterToWatchedRows(Set<RowReference> rowReferenceSet) {
        return rowReferenceSet.stream().filter(interestingRows::contains).collect(Collectors.toSet());
    }

    @Override
    public void enableWatchForRowPrefix(RowPrefixReference prefixReference) {
        throw new UnsupportedOperationException("nein");
    }

    @Override
    public void enableWatchForCells(Set<CellReference> cellReference) {
        throw new UnsupportedOperationException("Not yet implemented! Sorry");
    }

    @Override
    public Set<CellReference> disableWatchForCells(Set<CellReference> cellReference) {
        throw new UnsupportedOperationException("Not yet implemented! Sorry");
    }

    @Override
    public Set<CellReference> filterToWatchedCells(Set<CellReference> rowReferenceSet) {
        throw new UnsupportedOperationException("Not yet implemented! Sorry");
    }

    // TODO (jkong): Remove this black magic
    @Deprecated
    @VisibleForTesting
    Map<Cell, Value> getRows(TableReference tableRef,
            Iterable<byte[]> rows,
            ColumnSelection columnSelection,
            long timestamp) {
        Map<Cell, Value> result = Maps.newHashMap();
        Set<byte[]> skippedRows = Sets.newTreeSet(UnsignedBytes.lexicographicalComparator());

        List<byte[]> rowList = StreamSupport.stream(rows.spliterator(), false).collect(Collectors.toList());

        Map<RowReference, WatchIdentifierAndState> rowState = remoteLockWatchClient.getStateForRows(
                rowList.stream()
                        .map(r -> ImmutableRowReference.builder().tableReference(tableRef).row(r).build())
                        .filter(interestingRows::contains)
                        .collect(Collectors.toSet()));

        for (Map.Entry<RowReference, WatchIdentifierAndState> entry : rowState.entrySet()) {
            Optional<Map<Cell, Value>> cache =
                    rowStateCache.get(entry.getKey(), entry.getValue(), timestamp);
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
}
