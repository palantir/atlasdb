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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.logsafe.UnsafeArg;

public class RowStateCache {
    private static final Logger log = LoggerFactory.getLogger(RowStateCache.class);

    private final KeyValueService keyValueService;
    private final TransactionService transactionService;

    @VisibleForTesting
    final ConcurrentMap<RowCacheReference, RowStateCacheValue> backingMap;

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
            RowCacheReference cacheReference, WatchIdentifierAndState readState, long startTimestamp) {
        RowStateCacheValue currentCacheValue = backingMap.get(cacheReference);
        if (currentCacheValue == null) {
            // Nothing is cached, so we don't know
            updateCache(cacheReference, readState, startTimestamp);
            return Optional.empty();
        }

        CacheValidity validity = currentCacheValue.validityConditions().evaluate(readState, startTimestamp);

        switch(validity) {
            case USABLE:
                return Optional.of(currentCacheValue.data());
            case VALID_BUT_NOT_USABLE_HERE:
                return Optional.empty();
            case NO_LONGER_VALID:
                updateCache(cacheReference, readState, startTimestamp);
                return Optional.empty();
            default:
                throw new AssertionError("wat");
        }
    }

    private void updateCache(RowCacheReference rowReference, WatchIdentifierAndState readState, long readTimestamp) {
        // TODO (jkong): We need some way to handle this under high load
        RowStateCacheValue currentCacheValue = backingMap.get(rowReference);
        if (currentCacheValue != null) {
            if (currentCacheValue.validityConditions().watchIdentifierAndState().isAtLeastNewerThan(readState)) {
                return;
            }
        }

        // there is no currently cached value, so update
        updater.apply(ImmutableRowCacheUpdateRequest.builder()
                .cacheReference(rowReference)
                .watchIndexState(readState)
                .readTimestamp(readTimestamp)
                .build());
    }

    void ensureCacheFlushed() {
        try {
            updater.apply(new FakeRowCacheUpdateRequest()).get();
            LoggerFactory.getLogger(RowStateCache.class).info("We think we have flushed the cache, current"
                    + " state is {}", backingMap);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            LoggerFactory.getLogger(RowStateCache.class).info("We failed to flush the cache, current"
                    + " state is {}", backingMap, e);
            throw new RuntimeException(e.getCause());
        }
    }

    class UpdateFunction implements CoalescingRequestFunction<RowCacheUpdateRequest, Void> {
        @Override
        public Void defaultValue() {
            return null;
        }

        @Override
        public Map<RowCacheUpdateRequest, Void> apply(Set<RowCacheUpdateRequest> requests) {
            updateCacheDangerous(requests);
            log.info("Processed cache update requests: {}", UnsafeArg.of("requests", requests));
            return Maps.newHashMap();
        }
    }

    private void updateCacheDangerous(Set<RowCacheUpdateRequest> cacheUpdateRequests) {
        Map<TableReference, List<RowCacheUpdateRequest>> updatesByTable
                = cacheUpdateRequests.stream()
                .filter(request -> !(request instanceof FakeRowCacheUpdateRequest)) // TODO (jkong): Hacky test fixture
                .collect(Collectors.groupingBy(x -> x.cacheReference().tableReference()));

        for (Map.Entry<TableReference, List<RowCacheUpdateRequest>> entry : updatesByTable.entrySet()) {
            for (RowCacheUpdateRequest request : entry.getValue()) {
                RowStateCacheValue cacheValue = getCacheValue(request);
                backingMap.merge(request.cacheReference(), cacheValue, (existing, current) -> {
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

    private RowStateCacheValue getCacheValue(RowCacheUpdateRequest updateRequest) {
        if (updateRequest.cacheReference().rowReference().isPresent()) {
            RowCacheReference cacheReference = updateRequest.cacheReference();
            RowReference rowReference = cacheReference.rowReference().get();
            Map<Cell, Value> data = keyValueService.getRows(rowReference.tableReference(),
                    ImmutableList.of(rowReference.row()),
                    ColumnSelection.all(),
                    Long.MAX_VALUE);

            // TODO (jkong): This is O(N), we can do this in O(1) but hackweek and I'm lazy
            List<Long> interestingTimestamps = data.values().stream().map(Value::getTimestamp)
                    .collect(Collectors.toList());

            // TODO (jkong): Need some way of handling failure to commit. Probably do the usual lookback you do
            // in Atlas.
            long timestampAtWhichCachedDataWasFirstValid =
                    interestingTimestamps.size() == 0 ? updateRequest.readTimestamp() :
                            transactionService.get(interestingTimestamps).values().stream().mapToLong(x -> x).max()
                                    .orElse(Long.MAX_VALUE); // nothing committed, so it's only valid at infinity

            CacheEntryValidityConditions validity = ImmutableCacheEntryValidityConditions.builder()
                    .firstTimestampAtWhichReadIsValid(timestampAtWhichCachedDataWasFirstValid)
                    .watchIdentifierAndState(updateRequest.watchIndexState())
                    .build();
            RowStateCacheValue cacheValue = ImmutableRowStateCacheValue.builder()
                    .data(data)
                    .validityConditions(validity)
                    .build();
            return cacheValue;
        }

        if (updateRequest.cacheReference().prefixReference().isPresent()) {
            RowCacheReference cacheReference = updateRequest.cacheReference();
            RowPrefixReference prefixReference = cacheReference.prefixReference().get();
            Stream<RowResult<Value>> rowResults = keyValueService.getRange(
                    prefixReference.tableReference(),
                    RangeRequest.builder().prefixRange(prefixReference.prefix()).build(),
                    Long.MAX_VALUE).stream();
            Map<Cell, Value> data = Maps.newHashMap();
            rowResults.forEach(rowResult -> {
                for (Map.Entry<Cell, Value> entry : rowResult.getCells()) {
                    data.put(entry.getKey(), entry.getValue());
                }
            });

            // TODO (jkong): This is O(N), we can do this in O(1) but hackweek and I'm lazy
            List<Long> interestingTimestamps = data.values().stream().map(Value::getTimestamp)
                    .collect(Collectors.toList());

            // TODO (jkong): Need some way of handling failure to commit. Probably do the usual lookback you do
            // in Atlas.
            long timestampAtWhichCachedDataWasFirstValid =
                    interestingTimestamps.size() == 0 ? updateRequest.readTimestamp() :
                            transactionService.get(interestingTimestamps).values().stream().mapToLong(x -> x).max()
                                    .orElse(Long.MAX_VALUE); // nothing committed, so it's only valid at infinity

            CacheEntryValidityConditions validity = ImmutableCacheEntryValidityConditions.builder()
                    .firstTimestampAtWhichReadIsValid(timestampAtWhichCachedDataWasFirstValid)
                    .watchIdentifierAndState(updateRequest.watchIndexState())
                    .build();
            RowStateCacheValue cacheValue = ImmutableRowStateCacheValue.builder()
                    .data(data)
                    .validityConditions(validity)
                    .build();
            return cacheValue;
        }

        throw new AssertionError("wat");
    }

    @org.immutables.value.Value.Immutable
    public interface RowCacheUpdateRequest {
        RowCacheReference cacheReference();
        WatchIdentifierAndState watchIndexState();
        long readTimestamp();
    }

    private class FakeRowCacheUpdateRequest implements RowCacheUpdateRequest {
        @Override
        public RowCacheReference cacheReference() {
            return null;
        }

        @Override
        public WatchIdentifierAndState watchIndexState() {
            return null;
        }

        @Override
        public long readTimestamp() {
            return 0;
        }
    }
}
