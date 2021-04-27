/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.lock.cache.AbstractLockWatchValueCache;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionUpdate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.immutables.value.Value;

/**
 * This class batches getCommitTimestamps requests to TimeLock server for a single client/namespace.
 * */
final class BatchingCommitTimestampGetter<T> implements CommitTimestampGetter<T> {
    private final DisruptorAutobatcher<Request<T>, Long> autobatcher;

    private BatchingCommitTimestampGetter(DisruptorAutobatcher<Request<T>, Long> autobatcher) {
        this.autobatcher = autobatcher;
    }

    public static <T> BatchingCommitTimestampGetter<T> create(
            LockLeaseService leaseService,
            LockWatchEventCache eventCache,
            AbstractLockWatchValueCache<T, ?> valueCache) {
        DisruptorAutobatcher<Request<T>, Long> autobatcher = Autobatchers.independent(
                        consumer(leaseService, eventCache, valueCache))
                .safeLoggablePurpose("get-commit-timestamp")
                .build();
        return new BatchingCommitTimestampGetter<T>(autobatcher);
    }

    @Override
    public long getCommitTimestamp(long startTs, LockToken commitLocksToken, T transactionDigest) {
        return AtlasFutures.getUnchecked(autobatcher.apply(ImmutableRequest.<T>builder()
                .startTs(startTs)
                .commitLocksToken(commitLocksToken)
                .transactionDigest(transactionDigest)
                .build()));
    }

    @VisibleForTesting
    static <T> Consumer<List<BatchElement<Request<T>, Long>>> consumer(
            LockLeaseService leaseService,
            LockWatchEventCache eventCache,
            AbstractLockWatchValueCache<T, ?> valueCache) {
        return batch -> {
            int count = batch.size();
            List<Long> commitTimestamps = new ArrayList<>();
            while (commitTimestamps.size() < count) {
                Optional<LockWatchVersion> requestedVersion = eventCache.lastKnownVersion();
                GetCommitTimestampsResponse response =
                        leaseService.getCommitTimestamps(requestedVersion, count - commitTimestamps.size());
                commitTimestamps.addAll(
                        process(batch.subList(commitTimestamps.size(), count), response, eventCache, valueCache));
                LockWatchLogUtility.logTransactionEvents(requestedVersion, response.getLockWatchUpdate());
            }

            for (int i = 0; i < count; i++) {
                batch.get(i).result().set(commitTimestamps.get(i));
            }
        };
    }

    private static <T> List<Long> process(
            List<BatchElement<Request<T>, Long>> requests,
            GetCommitTimestampsResponse response,
            LockWatchEventCache eventCache,
            AbstractLockWatchValueCache<T, ?> valueCache) {
        List<Long> timestamps = LongStream.rangeClosed(response.getInclusiveLower(), response.getInclusiveUpper())
                .boxed()
                .collect(Collectors.toList());
        List<TransactionUpdate> transactionUpdates = Streams.zip(
                        timestamps.stream(), requests.stream(), (commitTs, batchElement) -> TransactionUpdate.builder()
                                .startTs(batchElement.argument().startTs())
                                .commitTs(commitTs)
                                .writesToken(batchElement.argument().commitLocksToken())
                                .build())
                .collect(Collectors.toList());
        eventCache.processGetCommitTimestampsUpdate(transactionUpdates, response.getLockWatchUpdate());
        requests.forEach(request -> valueCache.updateCacheOnCommit(
                request.argument().transactionDigest(), request.argument().startTs()));
        return timestamps;
    }

    @Override
    public void close() {
        autobatcher.close();
    }

    @Value.Immutable
    interface Request<T> {
        long startTs();

        LockToken commitLocksToken();

        T transactionDigest();
    }
}
