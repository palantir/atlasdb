/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.sweep;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.common.base.Throwables;

public final class CommitTsCache {
    private static final Long ONE_MILLION = 1_000_000L;
    private LoadingCache<Long, Long> cache;

    private CommitTsCache(TransactionService transactionService, long maxSize) {
        cache = CacheBuilder.newBuilder().maximumSize(maxSize).build(new AbortingCommitTsLoader(transactionService));
    }

    public static CommitTsCache create(TransactionService transactionService) {
        return new CommitTsCache(transactionService, ONE_MILLION);
    }

    public Optional<Long> loadIfCached(long startTs) {
        return Optional.ofNullable(cache.getIfPresent(startTs));
    }

    public long load(long startTs) {
        return cache.getUnchecked(startTs);
    }

    /**
     * Loads the commit timestamps for a batch of timestamps. Potentially reduces the number of kvs accesses since it
     * does batched lookups for non-cached start timestamps.
     */
    public Map<Long, Long> loadBatch(Collection<Long> timestamps) {
        try {
            return cache.getAll(timestamps);
        } catch (ExecutionException e) {
            throw Throwables.rewrapAndThrowUncheckedException(e);
        }
    }
}
