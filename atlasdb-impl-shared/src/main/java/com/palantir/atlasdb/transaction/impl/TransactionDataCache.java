/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.transaction.impl;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.immutables.value.Value;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TransactionService;
import com.palantir.atlasdb.protos.generated.TransactionService.TableCell;
import com.palantir.atlasdb.protos.generated.TransactionService.TimestampRange;
import com.palantir.atlasdb.timelock.hackweek.JamesTransactionService;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.util.Pair;

import io.vavr.collection.HashMap;
import io.vavr.collection.Map;

public final class TransactionDataCache {
    private static final int MAX_SIZE = 100_000_000;

    private final List<CacheKey> evictions = new ArrayList<>();
    private final Cache<CacheKey, Integer> evictionPolicy = Caffeine.newBuilder()
            .maximumWeight(MAX_SIZE)
            .weigher((CacheKey key, Integer size) ->
                    key.cell().getRowName().length + key.cell().getColumnName().length + size)
            .executor(MoreExecutors.directExecutor())
            .removalListener((key, value, cause) -> {
                if (cause.wasEvicted()) {
                    evictions.add(key);
                }
            })
            .build();

    private static final Interner<TableReference> tableInterner = Interners.newStrongInterner();
    private volatile AtlasCache cache = new AtlasCache(-1, HashMap.empty());

    private final TransactionManager transactionManager;
    private final JamesTransactionService txnService;

    public TransactionDataCache(
            TransactionManager transactionManager,
            JamesTransactionService txnService) {
        this.transactionManager = transactionManager;
        this.txnService = txnService;
    }

    public AtlasCache getCache() {
        return cache;
    }

    private synchronized void handleEvictions() {
        cache = new AtlasCache(cache.upToDateTo, cache.cache.removeAll(evictions));
        evictions.clear();
    }

    public synchronized void update() {
        Pair<List<CacheKey>, AtlasCache> newCache = invalidate(cache);
        evictionPolicy.invalidateAll(newCache.lhSide);
        cache = newCache.rhSide;
        handleEvictions();
    }

    private Pair<List<CacheKey>, AtlasCache> invalidate(AtlasCache from) {
        // add cache.upToDateTo
        TimestampRange range = txnService.startTransactions(cache.upToDateTo, 1);
        txnService.commitWrites(range.getLower(), emptyList());
        txnService.checkReadConflicts(range.getLower(), emptyList(), emptyList());
        txnService.unlock(singletonList(range.getLower()));
        List<TableCell> cells = range.getCacheUpdatesList();
        List<CacheKey> keys = cells.stream().map(tableCell -> ImmutableCacheKey.builder()
                .tableRef(tableRef(tableCell.getTable()))
                .cell(cell(tableCell.getCell()))
                .build())
                .collect(toList());
        Map<CacheKey, byte[]> newCache = from.cache.removeAll(keys);
        return Pair.create(keys, new AtlasCache(range.getLower(), newCache));
    }

    public synchronized void registerCacheMisses(TableReference tableReference, Set<Cell> cells) {
        Set<CacheKey> cellsToLoad = cells.stream().map(cell -> ImmutableCacheKey.builder().tableRef(tableReference).cell(cell).build()).collect(
                Collectors.toSet());
        AtlasCache loaded = transactionManager.runTaskReadOnly(txn -> {
            return new AtlasCache(((CachingTransaction) txn).delegate().getTimestamp(),
                    HashMap.ofAll(((CachingTransaction) txn).delegate().get(tableReference,
                            Sets.difference(cellsToLoad, cache.cache.keySet().toJavaSet()).stream().map(CacheKey::cell).collect(
                                    Collectors.toSet())).entrySet().stream(), entry ->
                                    ImmutableCacheKey.builder()
                                            .cell(entry.getKey())
                                            .tableRef(tableInterner.intern(tableReference)).build(),
                            java.util.Map.Entry::getValue));
        });
        AtlasCache merged = new AtlasCache(cache.upToDateTo, cache.cache.merge(loaded.cache));
        Pair<List<CacheKey>, AtlasCache> newCache = invalidate(merged);
        evictionPolicy.invalidateAll(newCache.lhSide);
        cache = newCache.rhSide;
        handleEvictions();
    }

    private static Cell cell(com.palantir.atlasdb.protos.generated.TransactionService.Cell cell) {
        return Cell.create(cell.getRow().toByteArray(), cell.getColumn().toByteArray());
    }

    private static TableReference tableRef(TransactionService.Table table) {
        return tableInterner.intern(TableReference.create(Namespace.create(table.getNamespace()), table.getTableName()));
    }

    @Value.Immutable
    public interface CacheKey {
        TableReference tableRef();
        Cell cell();
    }

    public static class AtlasCache {
        private final long upToDateTo;
        private final Map<CacheKey, byte[]> cache;

        private AtlasCache(long upToDateTo,
                Map<CacheKey, byte[]> cache) {
            this.upToDateTo = upToDateTo;
            this.cache = cache;
        }

        public long getUpToDateTo() {
            return upToDateTo;
        }

        public Map<CacheKey, byte[]> invalidate(TimestampRange timestampRange) {
            List<TableCell> cells = timestampRange.getCacheUpdatesList();
            List<CacheKey> keys = cells.stream().map(tableCell -> ImmutableCacheKey.builder()
                    .tableRef(tableRef(tableCell.getTable()))
                    .cell(cell(tableCell.getCell()))
                    .build())
                    .collect(toList());
            return cache.removeAll(keys);
        }
    }

}
