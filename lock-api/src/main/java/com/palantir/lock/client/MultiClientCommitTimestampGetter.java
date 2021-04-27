/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

import static com.palantir.lock.client.ConjureLockRequests.toConjure;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.cache.AbstractLockWatchValueCache;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionUpdate;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.immutables.value.Value;

public final class MultiClientCommitTimestampGetter<T> implements AutoCloseable {
    private final DisruptorAutobatcher<NamespacedRequest<T>, Long> autobatcher;

    private MultiClientCommitTimestampGetter(DisruptorAutobatcher<NamespacedRequest<T>, Long> autobatcher) {
        this.autobatcher = autobatcher;
    }

    public static <T> MultiClientCommitTimestampGetter<T> create(InternalMultiClientConjureTimelockService delegate) {
        DisruptorAutobatcher<NamespacedRequest<T>, Long> autobatcher =
                Autobatchers.<NamespacedRequest<T>, Long>independent(consumer(delegate))
                        .safeLoggablePurpose("multi-client-commit-timestamp-getter")
                        .build();
        return new MultiClientCommitTimestampGetter<T>(autobatcher);
    }

    public long getCommitTimestamp(
            Namespace namespace,
            long startTs,
            LockToken commitLocksToken,
            T transactionDigest,
            LockWatchEventCache eventCache,
            AbstractLockWatchValueCache<T, ?> valueCache) {
        return AtlasFutures.getUnchecked(autobatcher.apply(ImmutableNamespacedRequest.<T>builder()
                .namespace(namespace)
                .startTs(startTs)
                .transactionDigest(transactionDigest)
                .commitLocksToken(commitLocksToken)
                .eventCache(eventCache)
                .valueCache(valueCache)
                .build()));
    }

    @VisibleForTesting
    static <T> Consumer<List<BatchElement<NamespacedRequest<T>, Long>>> consumer(
            InternalMultiClientConjureTimelockService delegate) {
        return batch -> {
            BatchStateManager<T> batchStateManager = BatchStateManager.createFromRequestBatch(batch);
            while (batchStateManager.hasPendingRequests()) {
                batchStateManager.processResponse(delegate.getCommitTimestamps(batchStateManager.getRequests()));
            }
        };
    }

    private static final class BatchStateManager<T> {
        private final Map<Namespace, NamespacedBatchStateManager<T>> requestMap;

        private BatchStateManager(Map<Namespace, NamespacedBatchStateManager<T>> requestMap) {
            this.requestMap = requestMap;
        }

        static <T> BatchStateManager<T> createFromRequestBatch(List<BatchElement<NamespacedRequest<T>, Long>> batch) {
            Map<Namespace, NamespacedBatchStateManager<T>> requestMap = new HashMap<>();

            for (BatchElement<NamespacedRequest<T>, Long> elem : batch) {
                NamespacedRequest<T> argument = elem.argument();
                Namespace namespace = argument.namespace();
                NamespacedBatchStateManager<T> namespacedBatchStateManager = requestMap.computeIfAbsent(
                        namespace,
                        _unused -> new NamespacedBatchStateManager<>(argument.eventCache(), argument.valueCache()));
                namespacedBatchStateManager.addRequest(elem);
            }

            return new BatchStateManager<>(requestMap);
        }

        private boolean hasPendingRequests() {
            return requestMap.values().stream().anyMatch(NamespacedBatchStateManager::hasPendingRequests);
        }

        private Map<Namespace, GetCommitTimestampsRequest> getRequests() {
            return KeyedStream.stream(requestMap)
                    .filter(NamespacedBatchStateManager::hasPendingRequests)
                    .map(NamespacedBatchStateManager::getRequestForServer)
                    .collectToMap();
        }

        private void processResponse(Map<Namespace, GetCommitTimestampsResponse> responseMap) {
            responseMap.forEach((namespace, getCommitTimestampsResponse) ->
                    requestMap.get(namespace).serviceRequests(getCommitTimestampsResponse));
        }
    }

    private static final class NamespacedBatchStateManager<T> {
        private final Queue<BatchElement<NamespacedRequest<T>, Long>> pendingRequestQueue;
        private final LockWatchEventCache eventCache;
        private final AbstractLockWatchValueCache<T, ?> valueCache;
        private Optional<LockWatchVersion> lastKnownVersion;

        private NamespacedBatchStateManager(LockWatchEventCache cache, AbstractLockWatchValueCache<T, ?> valueCache) {
            this.pendingRequestQueue = new ArrayDeque<>();
            this.eventCache = cache;
            this.valueCache = valueCache;
            this.lastKnownVersion = Optional.empty();
        }

        private boolean hasPendingRequests() {
            return !pendingRequestQueue.isEmpty();
        }

        private void addRequest(BatchElement<NamespacedRequest<T>, Long> elem) {
            pendingRequestQueue.add(elem);
        }

        private GetCommitTimestampsRequest getRequestForServer() {
            return GetCommitTimestampsRequest.builder()
                    .numTimestamps(pendingRequestQueue.size())
                    .lastKnownVersion(toConjure(updateAndGetLastKnownVersion()))
                    .build();
        }

        private Optional<LockWatchVersion> updateAndGetLastKnownVersion() {
            lastKnownVersion = eventCache.lastKnownVersion();
            return lastKnownVersion;
        }

        private void serviceRequests(GetCommitTimestampsResponse commitTimestampsResponse) {
            List<Long> commitTimestamps = getCommitTimestampValues(commitTimestampsResponse);

            processLockWatchUpdate(commitTimestamps, commitTimestampsResponse.getLockWatchUpdate());
            LockWatchLogUtility.logTransactionEvents(lastKnownVersion, commitTimestampsResponse.getLockWatchUpdate());

            for (Long commitTimestamp : commitTimestamps) {
                pendingRequestQueue.poll().result().set(commitTimestamp);
            }
        }

        private List<Long> getCommitTimestampValues(GetCommitTimestampsResponse commitTimestampsResponse) {
            return LongStream.rangeClosed(
                            commitTimestampsResponse.getInclusiveLower(), commitTimestampsResponse.getInclusiveUpper())
                    .boxed()
                    .collect(Collectors.toList());
        }

        private void processLockWatchUpdate(List<Long> timestamps, LockWatchStateUpdate lockWatchUpdate) {
            List<TransactionUpdate> transactionUpdates = Streams.zip(
                            timestamps.stream(),
                            pendingRequestQueue.stream(),
                            (commitTs, batchElement) -> TransactionUpdate.builder()
                                    .startTs(batchElement.argument().startTs())
                                    .commitTs(commitTs)
                                    .writesToken(batchElement.argument().commitLocksToken())
                                    .build())
                    .collect(Collectors.toList());
            eventCache.processGetCommitTimestampsUpdate(transactionUpdates, lockWatchUpdate);
            pendingRequestQueue.stream()
                    .map(BatchElement::argument)
                    .forEach(request -> valueCache.updateCacheOnCommit(request.transactionDigest(), request.startTs()));
        }
    }

    @Override
    public void close() {
        autobatcher.close();
    }

    @Value.Immutable
    interface NamespacedRequest<T> {
        Namespace namespace();

        long startTs();

        T transactionDigest();

        LockToken commitLocksToken();

        LockWatchEventCache eventCache();

        AbstractLockWatchValueCache<T, ?> valueCache();
    }
}
