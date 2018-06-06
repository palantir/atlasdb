/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.sweep;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.logsafe.SafeArg;

import gnu.trove.TDecorators;
import gnu.trove.map.TLongLongMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.set.TLongSet;

public final class CommitTsLoader {
    private static final Logger log = LoggerFactory.getLogger(CommitTsLoader.class);

    private static final int TS_PER_BATCH = 5000;

    private final TLongLongMap commitTsByStartTs;
    private final TransactionService transactionService;

    private CommitTsLoader(TLongLongMap commitTsByStartTs, TransactionService transactionService) {
        this.commitTsByStartTs = commitTsByStartTs;
        this.transactionService = transactionService;
    }

    public static CommitTsLoader create(TransactionService transactionService, TLongSet startTssToWarmingCache) {
        TLongLongMap cache = new TLongLongHashMap();
        if (!startTssToWarmingCache.isEmpty()) {
            warmCache(transactionService, startTssToWarmingCache, cache);

        }
        return new CommitTsLoader(cache, transactionService);
    }

    private static void warmCache(
            TransactionService transactionService,
            TLongSet startTssToWarmingCache,
            TLongLongMap cache) {
        // Ideally TransactionService should work with primitive collections to avoid GC overhead.
        for (List<Long> longList : Iterables.partition(TDecorators.wrap(startTssToWarmingCache), TS_PER_BATCH)) {
            cache.putAll(transactionService.get(longList));
        }
    }

    public long load(long startTs) {
        if (!commitTsByStartTs.containsKey(startTs)) {
            long commitTs = loadCacheMissAndPossiblyRollBack(startTs);
            commitTsByStartTs.put(startTs, commitTs);
        }
        return commitTsByStartTs.get(startTs);
    }

    /**
     * Loads the commit timestamps for a batch of timestamps. Potentially reduces the number of kvs accesses since it
     * only does a single lookup for all the non-cached start timestamps.
     */
    public Map<Long, Long> loadBatch(Collection<Long> timestamps) {
        List<Long> missingKeys = new ArrayList<>();
        Map<Long, Long> result = new HashMap<>();

        for (long startTs: timestamps) {
            if (!commitTsByStartTs.containsKey(startTs)) {
                missingKeys.add(startTs);
            } else {
                result.put(startTs, commitTsByStartTs.get(startTs));
            }
        }

        // load all committed transactions that weren't already cached
        Map<Long, Long> nonCachedCommittedTransactions = transactionService.get(missingKeys);
        result.putAll(nonCachedCommittedTransactions);
        commitTsByStartTs.putAll(nonCachedCommittedTransactions);

        // roll back any uncommitted transactions that weren't already cached
        missingKeys.stream()
                .filter(startTs -> !nonCachedCommittedTransactions.containsKey(startTs))
                .forEach(startTs -> result.put(startTs, load(startTs)));

        return result;
    }

    public long loadCacheMissAndPossiblyRollBack(long startTs) {
        Long commitTs = transactionService.get(startTs);

        if (commitTs != null) {
            return commitTs;
        }

        // Roll back this transaction (note that rolling back arbitrary transactions
        // can never cause correctness issues, only liveness issues)
        try {
            // TODO(carrino): use the batched version of putUnlessExists when it is available.
            transactionService.putUnlessExists(startTs, TransactionConstants.FAILED_COMMIT_TS);
        } catch (KeyAlreadyExistsException e) {
            String msg = "Could not roll back transaction with start timestamp " + startTs + "; either"
                    + " it was already rolled back (by a different transaction), or it committed successfully"
                    + " before we could roll it back.";
            log.warn("This isn't a bug but it should be very infrequent. {}", msg,
                    new TransactionFailedRetriableException(msg, e));
        }

        Long commitTsAfterRollBack = transactionService.get(startTs);
        if (commitTsAfterRollBack != null) {
            return commitTsAfterRollBack;
        } else {
            // This can happen if the clean tx table CLI has rolled-back the transaction at the start ts.
            log.warn("Did not find a commitTs for startTs {} after a rollback. "
                    + "This is possibly due to a delete at the above startTs via the clean transaction table CLI.",
                    SafeArg.of("startTs", startTs));
            return TransactionConstants.FAILED_COMMIT_TS;
        }
    }
}
