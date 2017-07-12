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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionService;

import gnu.trove.TDecorators;
import gnu.trove.map.TLongLongMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.set.TLongSet;

public final class CommitTsLoader {
    private static final Logger log = LoggerFactory.getLogger(CommitTsLoader.class);

    private final TLongLongMap commitTsByStartTs;
    private final TransactionService transactionService;

    private CommitTsLoader(TLongLongMap commitTsByStartTs, TransactionService transactionService) {
        this.commitTsByStartTs = commitTsByStartTs;
        this.transactionService = transactionService;
    }

    public static CommitTsLoader create(TransactionService transactionService, TLongSet startTssToWarmingCache) {
        TLongLongMap cache = new TLongLongHashMap();
        if (!startTssToWarmingCache.isEmpty()) {
            // Ideally TransactionService should work with primitive collections to avoid GC overhead..
            cache.putAll(transactionService.get(TDecorators.wrap(startTssToWarmingCache)));
        }
        return new CommitTsLoader(cache, transactionService);
    }

    public long load(long startTs) {
        if (!commitTsByStartTs.containsKey(startTs)) {
            long commitTs = loadCacheMissAndPossiblyRollBack(startTs);
            commitTsByStartTs.put(startTs, commitTs);
        }
        return commitTsByStartTs.get(startTs);
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

        return transactionService.get(startTs);
    }
}
