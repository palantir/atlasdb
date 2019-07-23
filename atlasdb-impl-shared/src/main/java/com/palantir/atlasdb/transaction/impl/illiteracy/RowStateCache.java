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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.transaction.service.TransactionService;

public class RowStateCache {
    private final KeyValueService keyValueService;
    private final TransactionService transactionService;

    @VisibleForTesting
    final ConcurrentMap<RowReference, RowStateCacheValue> backingMap;

    @VisibleForTesting
    final DisruptorAutobatcher<RowCacheUpdateRequest, Void> updater;

    public RowStateCache(KeyValueService keyValueService,
            TransactionService transactionService) {
        this.keyValueService = keyValueService;
        this.transactionService = transactionService;
        this.backingMap = Maps.newConcurrentMap();
        this.updater = Autobatchers.coalescing(new UpdateFunction()).safeLoggablePurpose("row-state-cache").build();
    }

    public Optional<Map<Cell, Value>> get(
            RowReference rowReference, WatchIdentifierAndState readState, long startTimestamp) {
        RowStateCacheValue currentCacheValue = backingMap.get(rowReference);
        if (currentCacheValue == null) {
            // Nothing is cached, so we don't know
            updateCache(rowReference, readState, startTimestamp);
            return Optional.empty();
        }

        CacheValidity validity = currentCacheValue.validityConditions().evaluate(readState, startTimestamp);

        switch(validity) {
            case USABLE:
                return Optional.of(currentCacheValue.data());
            case VALID_BUT_NOT_USABLE_HERE:
                return Optional.empty();
            case NO_LONGER_VALID:
                updateCache(rowReference, readState, startTimestamp);
                return Optional.empty();
            default:
                throw new AssertionError("wat");
        }
    }

    private void updateCache(RowReference rowReference, WatchIdentifierAndState readState, long readTimestamp) {
        // TODO (jkong): We need some way to handle this under high load
        RowStateCacheValue currentCacheValue = backingMap.get(rowReference);
        if (currentCacheValue != null) {
            if (currentCacheValue.validityConditions().watchIdentifierAndState().isAtLeastNewerThan(readState)) {
                return;
            }
        }

        // there is no currently cached value, so update
        updater.apply(ImmutableRowCacheUpdateRequest.builder()
                .rowReference(rowReference)
                .watchIndexState(readState)
                .readTimestamp(readTimestamp)
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

                // TODO (jkong): Need some way of handling failure to commit. Probably do the usual lookback you do
                // in Atlas.
                long timestampAtWhichCachedDataWasFirstValid =
                        interestingTimestamps.size() == 0 ? request.readTimestamp() :
                        transactionService.get(interestingTimestamps).values().stream().mapToLong(x -> x).max()
                                .orElse(Long.MAX_VALUE); // nothing committed, so it's only valid at infinity

                CacheEntryValidityConditions validity = ImmutableCacheEntryValidityConditions.builder()
                        .firstTimestampAtWhichReadIsValid(timestampAtWhichCachedDataWasFirstValid)
                        .watchIdentifierAndState(request.watchIndexState())
                        .build();
                RowStateCacheValue cacheValue = ImmutableRowStateCacheValue.builder()
                        .data(data)
                        .validityConditions(validity)
                        .build();

                backingMap.merge(request.rowReference(), cacheValue, (existing, current) -> {
                    if (!Objects.equals(current.validityConditions().watchIdentifierAndState().identifier(),
                            existing.validityConditions().watchIdentifierAndState().identifier())) {
                        return current;
                    }
                    // same identifiers
                    if (current.validityConditions().watchIdentifierAndState().isAtLeastNewerThan(
                            existing.validityConditions().watchIdentifierAndState())) {
                        return current;
                    }
                    return existing;
                });
            }
        }
    }

    @org.immutables.value.Value.Immutable
    public interface RowCacheUpdateRequest {
        RowReference rowReference();
        WatchIdentifierAndState watchIndexState();
        long readTimestamp();
    }
}
