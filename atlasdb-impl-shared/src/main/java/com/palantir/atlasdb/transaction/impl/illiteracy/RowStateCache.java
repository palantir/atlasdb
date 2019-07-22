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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.timelock.watch.WatchIndexState;
import com.palantir.atlasdb.transaction.service.TransactionService;

public class RowStateCache {
    private final KeyValueService keyValueService;
    private final TransactionService transactionService;
    private final ConcurrentMap<RowReference, RowStateCacheValue> backingMap;

    private final DisruptorAutobatcher<RowCacheUpdateRequest, Void> updater;

    public RowStateCache(KeyValueService keyValueService,
            TransactionService transactionService) {
        this.keyValueService = keyValueService;
        this.transactionService = transactionService;
        this.backingMap = Maps.newConcurrentMap();
        this.updater = Autobatchers.coalescing(new UpdateFunction()).safeLoggablePurpose("row-state-cache").build();
    }

    public Optional<Map<Cell, Value>> get(RowReference rowReference, WatchIndexState readState, long startTimestamp) {
        RowStateCacheValue currentCacheValue = backingMap.get(rowReference);
        if (currentCacheValue == null) {
            // Nothing is cached, so we don't know
            updateCache(rowReference, readState);
            return Optional.empty();
        }

        // there is a cache value, but things have changed since
        if (readState.compareTo(currentCacheValue.watchIndexState()) > 0) {
            updateCache(rowReference, readState);
            return Optional.empty();
        }

        // there is a cache value that is at our read or newer.
        if (currentCacheValue.earliestValidTimestamp() > startTimestamp) {
            // The most recent value actually committed after our start, so we can't use it.
            // To save memory we only remember one value
            // BUT in this case we DON'T update the cache as there's no reason to believe it's out of date.
            return Optional.empty();
        }

        return Optional.of(currentCacheValue.data());
    }

    private void updateCache(RowReference rowReference, WatchIndexState readState) {
        // TODO (jkong): We need some way to handle this under high load
        RowStateCacheValue currentCacheValue = backingMap.get(rowReference);
        if (currentCacheValue != null) {
            if (currentCacheValue.watchIndexState().compareTo(readState) >= 0) {
                return;
            }
            // the current value is stale, so update
        }
        // there is no currently cached value, so update
        updater.apply(ImmutableRowCacheUpdateRequest.builder()
                .rowReference(rowReference)
                .watchIndexState(readState)
                .build());
    }

    class UpdateFunction implements CoalescingRequestFunction<RowCacheUpdateRequest, Void> {
        @Override
        public Void defaultValue() {
            return null;
        }

        @Override
        public Map<RowCacheUpdateRequest, Void> apply(Set<RowCacheUpdateRequest> request) {
            updateCacheDangerous(request);
            return Maps.newHashMap();
        }
    }

    private void updateCacheDangerous(Set<RowCacheUpdateRequest> cacheUpdateRequests) {
        Map<TableReference, List<RowCacheUpdateRequest>> updatesByTable
                = cacheUpdateRequests.stream()
                .collect(Collectors.groupingBy(x -> x.rowReference().tableReference()));

        for (Map.Entry<TableReference, List<RowCacheUpdateRequest>> entry : updatesByTable.entrySet()) {
            Map<Cell, Value> data = keyValueService.getRows(entry.getKey(),
                    entry.getValue().stream().map(t -> t.rowReference().row()).collect(Collectors.toList()),
                    ColumnSelection.all(),
                    Long.MAX_VALUE);

            for (RowCacheUpdateRequest request : entry.getValue()) {
                // TODO (jkong): This is O(N), we can do this in O(1) but hackweek and I'm lazy
                Map<Cell, Value> valuesInRow = Maps.filterKeys(data, k -> Arrays.equals(request.rowReference().row(),
                        k.getRowName()));
                List<Long> interestingTimestamps = data.values().stream().map(Value::getTimestamp)
                        .collect(Collectors.toList());

                long timestampAtWhichCachedDataWasFirstValid =
                        transactionService.get(interestingTimestamps).values().stream().mapToLong(x -> x).max()
                                .orElse(0);
                RowStateCacheValue cacheValue = ImmutableRowStateCacheValue.builder()
                        .earliestValidTimestamp(timestampAtWhichCachedDataWasFirstValid)
                        .data(data)
                        .watchIndexState(request.watchIndexState())
                        .build();

                backingMap.merge(request.rowReference(), cacheValue, (existing, current) -> {
                    int cv = existing.watchIndexState().compareTo(current.watchIndexState());
                    if (cv > 0) {
                        return existing;
                    }
                    return current;
                });
            }
        }
    }

    @org.immutables.value.Value.Immutable
    interface RowCacheUpdateRequest {
        RowReference rowReference();
        WatchIndexState watchIndexState();
    }
}
