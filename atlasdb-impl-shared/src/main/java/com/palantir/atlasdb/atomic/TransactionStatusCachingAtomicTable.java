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

package com.palantir.atlasdb.atomic;

import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.cache.CommitStateCache;
import com.palantir.atlasdb.cache.DefaultCommitStateCache;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

// todo(snanda): gut wrenching layering
public class TransactionStatusCachingAtomicTable implements AtomicTable<Long, TransactionStatus> {
    private final AtomicTable<Long, TransactionStatus> delegate;
    private final CommitStateCache<TransactionStatus> cache;

    public TransactionStatusCachingAtomicTable(
            AtomicTable<Long, TransactionStatus> delegate, MetricRegistry metricRegistry, LongSupplier size) {
        this.delegate = delegate;
        this.cache = new DefaultCommitStateCache<>(metricRegistry, size);
    }

    @Override
    public void markInProgress(Iterable<Long> keys) {
        delegate.markInProgress(keys);
    }

    @Override
    public void updateMultiple(Map<Long, TransactionStatus> keyValues) throws KeyAlreadyExistsException {
        delegate.updateMultiple(keyValues);
    }

    @Override
    public ListenableFuture<Map<Long, TransactionStatus>> get(Iterable<Long> keys) {
        Set<Long> pendingGets = new HashSet<>();
        Map<Long, TransactionStatus> result = new HashMap<>();

        for (Long startTs : keys) {
            TransactionStatus commit = cache.getCommitStateIfPresent(startTs);
            if (commit == null) {
                pendingGets.add(startTs);
            } else {
                result.put(startTs, commit);
            }
        }

        if (pendingGets.isEmpty()) {
            return Futures.immediateFuture(result);
        }

        return Futures.transform(
                delegate.get(pendingGets),
                rawResults -> {
                    for (Map.Entry<Long, TransactionStatus> e : rawResults.entrySet()) {
                        if (e.getValue() != null) {
                            Long startTs = e.getKey();
                            TransactionStatus commit = e.getValue();
                            result.put(startTs, commit);
                            cache.putAlreadyCommittedTransaction(startTs, commit);
                        }
                    }

                    return result;
                },
                MoreExecutors.directExecutor());
    }
}
