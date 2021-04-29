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
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockWatchCache;
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

public final class MultiClientCommitTimestampGetter implements AutoCloseable {
    private final DisruptorAutobatcher<NamespacedRequest, Long> autobatcher;

    private MultiClientCommitTimestampGetter(DisruptorAutobatcher<NamespacedRequest, Long> autobatcher) {
        this.autobatcher = autobatcher;
    }

    public static MultiClientCommitTimestampGetter create(InternalMultiClientConjureTimelockService delegate) {
        DisruptorAutobatcher<NamespacedRequest, Long> autobatcher = Autobatchers.independent(consumer(delegate))
                .safeLoggablePurpose("multi-client-commit-timestamp-getter")
                .build();
        return new MultiClientCommitTimestampGetter(autobatcher);
    }

    public long getCommitTimestamp(
            Namespace namespace, long startTs, LockToken commitLocksToken, LockWatchCache cache) {
        return AtlasFutures.getUnchecked(autobatcher.apply(ImmutableNamespacedRequest.builder()
                .namespace(namespace)
                .startTs(startTs)
                .commitLocksToken(commitLocksToken)
                .cache(cache)
                .build()));
    }

    @VisibleForTesting
    static Consumer<List<BatchElement<NamespacedRequest, Long>>> consumer(
            InternalMultiClientConjureTimelockService delegate) {
        return batch -> {
            BatchStateManager batchStateManager = BatchStateManager.createFromRequestBatch(batch);
            while (batchStateManager.hasPendingRequests()) {
                batchStateManager.processResponse(delegate.getCommitTimestamps(batchStateManager.getRequests()));
            }
        };
    }

    private static final class BatchStateManager {
        private final Map<Namespace, NamespacedBatchStateManager> requestMap;

        private BatchStateManager(Map<Namespace, NamespacedBatchStateManager> requestMap) {
            this.requestMap = requestMap;
        }

        static BatchStateManager createFromRequestBatch(List<BatchElement<NamespacedRequest, Long>> batch) {
            Map<Namespace, NamespacedBatchStateManager> requestMap = new HashMap<>();

            for (BatchElement<NamespacedRequest, Long> elem : batch) {
                NamespacedRequest argument = elem.argument();
                Namespace namespace = argument.namespace();
                NamespacedBatchStateManager namespacedBatchStateManager = requestMap.computeIfAbsent(
                        namespace, _unused -> new NamespacedBatchStateManager(argument.cache()));
                namespacedBatchStateManager.addRequest(elem);
            }

            return new BatchStateManager(requestMap);
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

    private static final class NamespacedBatchStateManager {
        private final Queue<BatchElement<NamespacedRequest, Long>> pendingRequestQueue;
        private final LockWatchCache cache;
        private Optional<LockWatchVersion> lastKnownVersion;

        private NamespacedBatchStateManager(LockWatchCache cache) {
            this.pendingRequestQueue = new ArrayDeque<>();
            this.cache = cache;
            this.lastKnownVersion = Optional.empty();
        }

        private boolean hasPendingRequests() {
            return !pendingRequestQueue.isEmpty();
        }

        private void addRequest(BatchElement<NamespacedRequest, Long> elem) {
            pendingRequestQueue.add(elem);
        }

        private GetCommitTimestampsRequest getRequestForServer() {
            return GetCommitTimestampsRequest.builder()
                    .numTimestamps(pendingRequestQueue.size())
                    .lastKnownVersion(toConjure(updateAndGetLastKnownVersion()))
                    .build();
        }

        private Optional<LockWatchVersion> updateAndGetLastKnownVersion() {
            lastKnownVersion = cache.getEventCache().lastKnownVersion();
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
            cache.processCommitTimestampsUpdate(transactionUpdates, lockWatchUpdate);
        }
    }

    @Override
    public void close() {
        autobatcher.close();
    }

    @Value.Immutable
    interface NamespacedRequest {
        Namespace namespace();

        long startTs();

        LockToken commitLocksToken();

        LockWatchCache cache();
    }
}
