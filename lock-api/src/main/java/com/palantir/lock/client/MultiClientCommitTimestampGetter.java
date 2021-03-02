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
import com.palantir.lock.watch.ImmutableTransactionUpdate;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.TransactionUpdate;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.immutables.value.Value;

public class MultiClientCommitTimestampGetter implements AutoCloseable {
    private final DisruptorAutobatcher<NamespacedRequest, Long> autobatcher;

    private MultiClientCommitTimestampGetter(DisruptorAutobatcher<NamespacedRequest, Long> autobatcher) {
        this.autobatcher = autobatcher;
    }

    static MultiClientCommitTimestampGetter create(InternalMultiClientConjureTimelockService delegate) {
        DisruptorAutobatcher<NamespacedRequest, Long> autobatcher = Autobatchers.independent(consumer(delegate))
                .safeLoggablePurpose("multi-client-get-commit-timestamp")
                .build();
        return new MultiClientCommitTimestampGetter(autobatcher);
    }

    public long getCommitTimestamp(Namespace namespace, long startTs, LockToken commitLocksToken) {
        return AtlasFutures.getUnchecked(autobatcher.apply(ImmutableNamespacedRequest.builder()
                .namespace(namespace)
                .startTs(startTs)
                .commitLocksToken(commitLocksToken)
                .build()));
    }

    @VisibleForTesting
    static Consumer<List<BatchElement<NamespacedRequest, Long>>> consumer(
            InternalMultiClientConjureTimelockService delegate) {
        return batch -> processBatch(delegate, batch);
    }

    private static void processBatch(
            InternalMultiClientConjureTimelockService delegate, List<BatchElement<NamespacedRequest, Long>> batch) {
        BatchStateManager batchStateManager = new BatchStateManager(getNamespaceWiseBatchStateManager(batch));
        while (batchStateManager.hasPendingRequests()) {
            batchStateManager.processResponse(delegate.getCommitTimestamps(batchStateManager.getRequests()));
        }
    }

    private static Map<Namespace, NamespacedBatchStateManager> getNamespaceWiseBatchStateManager(
            List<BatchElement<NamespacedRequest, Long>> batch) {
        Map<Namespace, NamespacedBatchStateManager> requestMap = new HashMap<>();

        for (BatchElement<NamespacedRequest, Long> elem : batch) {
            NamespacedRequest argument = elem.argument();
            Namespace namespace = argument.namespace();
            NamespacedBatchStateManager namespacedBatchStateManager =
                    requestMap.computeIfAbsent(namespace, _n -> new NamespacedBatchStateManager(argument.cache()));
            namespacedBatchStateManager.addRequest(elem);
        }

        return requestMap;
    }

    private static class BatchStateManager {
        private final Map<Namespace, NamespacedBatchStateManager> requestMap;

        private BatchStateManager(Map<Namespace, NamespacedBatchStateManager> requestMap) {
            this.requestMap = requestMap;
        }

        boolean hasPendingRequests() {
            return requestMap.values().stream().anyMatch(NamespacedBatchStateManager::hasPendingRequests);
        }

        Map<Namespace, GetCommitTimestampsRequest> getRequests() {
            return KeyedStream.stream(requestMap)
                    .filter(NamespacedBatchStateManager::hasPendingRequests)
                    .map(NamespacedBatchStateManager::getPendingRequest)
                    .collectToMap();
        }

        void processResponse(Map<Namespace, GetCommitTimestampsResponse> namespaceWiseResponse) {
            KeyedStream.stream(namespaceWiseResponse)
                    .forEach((namespace, getCommitTimestampsResponse) ->
                            requestMap.get(namespace).serviceRequests(getCommitTimestampsResponse));
        }
    }

    private static class NamespacedBatchStateManager {
        private final Queue<BatchElement<NamespacedRequest, Long>> pendingRequestQueue;
        private final LockWatchEventCache cache;

        private NamespacedBatchStateManager(LockWatchEventCache cache) {
            this.pendingRequestQueue = new LinkedList();
            this.cache = cache;
        }

        boolean hasPendingRequests() {
            return !pendingRequestQueue.isEmpty();
        }

        void addRequest(BatchElement<NamespacedRequest, Long> elem) {
            pendingRequestQueue.add(elem);
        }

        GetCommitTimestampsRequest getPendingRequest() {
            return GetCommitTimestampsRequest.builder()
                    .numTimestamps(pendingRequestQueue.size())
                    .lastKnownVersion(toConjure(cache.lastKnownVersion()))
                    .build();
        }

        void serviceRequests(GetCommitTimestampsResponse commitTimestampsResponse) {
            List<Long> commitTimestamps = process(
                    (List<BatchElement<NamespacedRequest, Long>>) pendingRequestQueue, commitTimestampsResponse);
            Iterator<Long> iterator = commitTimestamps.iterator();
            while (iterator.hasNext()) {
                pendingRequestQueue.poll().result().set(iterator.next());
            }
        }

        // Todo snanda this is duplicated
        private List<Long> process(
                List<BatchElement<NamespacedRequest, Long>> requests, GetCommitTimestampsResponse response) {
            List<Long> timestamps = LongStream.rangeClosed(response.getInclusiveLower(), response.getInclusiveUpper())
                    .boxed()
                    .collect(Collectors.toList());
            List<TransactionUpdate> transactionUpdates = Streams.zip(
                            timestamps.stream(),
                            requests.stream(),
                            (commitTs, batchElement) -> ImmutableTransactionUpdate.builder()
                                    .startTs(batchElement.argument().startTs())
                                    .commitTs(commitTs)
                                    .writesToken(batchElement.argument().commitLocksToken())
                                    .build())
                    .collect(Collectors.toList());
            cache.processGetCommitTimestampsUpdate(transactionUpdates, response.getLockWatchUpdate());
            return timestamps;
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

        LockWatchEventCache cache();
    }
}
