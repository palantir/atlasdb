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

import static com.palantir.lock.client.ConjureLockRequests.fromConjure;
import static com.palantir.lock.client.ConjureLockRequests.toConjure;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher.DisruptorFuture;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.watch.LockWatchCache;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.immutables.value.Value;

public final class MultiClientTransactionStarter implements AutoCloseable {
    private final DisruptorAutobatcher<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>
            autobatcher;

    private MultiClientTransactionStarter(
            DisruptorAutobatcher<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>
                    autobatcher) {
        this.autobatcher = autobatcher;
    }

    public static MultiClientTransactionStarter create(InternalMultiClientConjureTimelockService delegate) {
        DisruptorAutobatcher<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>> autobatcher =
                Autobatchers.independent(consumer(delegate, UUID.randomUUID()))
                        .safeLoggablePurpose("multi-client-transaction-starter")
                        .timeoutHandler(exception -> new StartTransactionFailedException(
                                "Timed out while attempting to start transactions", exception))
                        .batchFunctionTimeout(Duration.ofSeconds(30))
                        .build();
        return new MultiClientTransactionStarter(autobatcher);
    }

    public List<StartIdentifiedAtlasDbTransactionResponse> startTransactions(
            Namespace namespace, int request, LockWatchCache cache, LockCleanupService lockCleanupService) {
        return AtlasFutures.getUnchecked(autobatcher.apply(
                NamespaceAndRequestParams.of(namespace, RequestParams.of(request, cache, lockCleanupService))));
    }

    private static Consumer<
                    List<BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>>>
            consumer(InternalMultiClientConjureTimelockService delegate, UUID requestorId) {
        return batch -> processBatch(delegate, requestorId, batch);
    }

    @VisibleForTesting
    static void processBatch(
            InternalMultiClientConjureTimelockService delegate,
            UUID requestorId,
            List<BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>> batch) {

        Map<Namespace, ResponseHandler> responseHandler = getResponseHandlers(batch);
        MultiClientRequestManager multiClientRequestManager =
                new MultiClientRequestManager(partitionRequestsByNamespace(batch));

        Map<Namespace, List<StartIdentifiedAtlasDbTransactionResponse>> startTransactionResponses;
        try {
            while (multiClientRequestManager.requestsPending()) {
                startTransactionResponses = getProcessedStartTransactionResponses(
                        multiClientRequestManager.requestMap, delegate, requestorId);
                startTransactionResponses.forEach((namespace, responseList) -> {
                    multiClientRequestManager.updatePendingStartTransactionsCount(namespace, responseList.size());
                    responseHandler.get(namespace).processResponse(responseList);
                });
            }
        } finally {
            clearResources(multiClientRequestManager, responseHandler);
        }
    }

    private static void clearResources(
            MultiClientRequestManager multiClientRequestManager, Map<Namespace, ResponseHandler> responseHandler) {
        multiClientRequestManager.close();
        responseHandler.forEach((_unused, handler) -> handler.close());
    }

    private static Map<Namespace, ResponseHandler> getResponseHandlers(
            List<BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>> batch) {
        Map<Namespace, ResponseHandler> responseHandlers = new HashMap<>();

        for (BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>> element : batch) {
            NamespaceAndRequestParams requestParams = element.argument();
            Namespace namespace = requestParams.namespace();
            responseHandlers
                    .computeIfAbsent(
                            namespace,
                            _unused ->
                                    new ResponseHandler(requestParams.params().lockCleanupService()))
                    .addPendingFuture(SettableResponse.of(requestParams.params().numTransactions(), element.result()));
        }

        return responseHandlers;
    }

    @VisibleForTesting
    static Map<Namespace, RequestParams> partitionRequestsByNamespace(
            List<BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>> batch) {
        return batch.stream()
                .map(BatchElement::argument)
                .collect(Collectors.toMap(
                        NamespaceAndRequestParams::namespace, NamespaceAndRequestParams::params, RequestParams::merge));
    }

    private static Map<Namespace, List<StartIdentifiedAtlasDbTransactionResponse>>
            getProcessedStartTransactionResponses(
                    Map<Namespace, RequestParams> originalRequestMap,
                    InternalMultiClientConjureTimelockService delegate,
                    UUID requestorId) {

        Map<Namespace, ConjureStartTransactionsRequest> requests = getConjureRequests(originalRequestMap, requestorId);
        Map<Namespace, ConjureStartTransactionsResponse> responseMap = getResponseMap(delegate, requests);

        Map<Namespace, List<StartIdentifiedAtlasDbTransactionResponse>> processedResult = new HashMap<>();
        for (Map.Entry<Namespace, ConjureStartTransactionsResponse> entry : responseMap.entrySet()) {
            Namespace namespace = entry.getKey();
            ConjureStartTransactionsResponse response = entry.getValue();
            TransactionStarterHelper.updateCacheWithStartTransactionResponse(
                    originalRequestMap.get(namespace).cache(),
                    fromConjure(requests.get(namespace).getLastKnownVersion()),
                    response);
            processedResult.put(namespace, TransactionStarterHelper.split(response));
        }
        return processedResult;
    }

    @VisibleForTesting
    static Map<Namespace, ConjureStartTransactionsRequest> getConjureRequests(
            Map<Namespace, RequestParams> originalRequestMap, UUID requestorId) {
        return KeyedStream.stream(originalRequestMap)
                .mapEntries((namespace, requestParams) ->
                        Maps.immutableEntry(namespace, getConjureRequest(requestParams, requestorId)))
                .collectToMap();
    }

    private static ConjureStartTransactionsRequest getConjureRequest(RequestParams requestParams, UUID requestorId) {
        return ConjureStartTransactionsRequest.builder()
                .requestorId(requestorId)
                .requestId(UUID.randomUUID())
                .numTransactions(requestParams.numTransactions())
                .lastKnownVersion(
                        toConjure(requestParams.cache().getEventCache().lastKnownVersion()))
                .build();
    }

    private static Map<Namespace, ConjureStartTransactionsResponse> getResponseMap(
            InternalMultiClientConjureTimelockService delegate,
            Map<Namespace, ConjureStartTransactionsRequest> conjureRequests) {
        return KeyedStream.stream(delegate.startTransactions(conjureRequests))
                .map(LockLeaseService::assignLeasedLockTokenToImmutableTimestampLock)
                .collectToMap();
    }

    @Override
    public void close() {
        autobatcher.close();
    }

    @Value.Immutable
    interface NamespaceAndRequestParams {
        @Value.Parameter
        Namespace namespace();

        @Value.Parameter
        RequestParams params();

        static NamespaceAndRequestParams of(Namespace namespace, RequestParams params) {
            return ImmutableNamespaceAndRequestParams.of(namespace, params);
        }
    }

    @Value.Immutable
    interface RequestParams {
        @Value.Parameter
        Integer numTransactions();

        @Value.Parameter
        LockWatchCache cache();

        @Value.Parameter
        LockCleanupService lockCleanupService();

        static RequestParams of(int numTransactions, LockWatchCache cache, LockCleanupService lockCleanupService) {
            return ImmutableRequestParams.of(numTransactions, cache, lockCleanupService);
        }

        static RequestParams merge(RequestParams params1, RequestParams params2) {
            return ImmutableRequestParams.copyOf(params1)
                    .withNumTransactions(params1.numTransactions() + params2.numTransactions());
        }
    }

    @Value.Immutable
    interface SettableResponse {
        @Value.Parameter
        Integer numTransactions();

        @Value.Parameter
        DisruptorFuture<List<StartIdentifiedAtlasDbTransactionResponse>> startTransactionsFuture();

        static SettableResponse of(
                int numTransactions, DisruptorFuture<List<StartIdentifiedAtlasDbTransactionResponse>> future) {
            return ImmutableSettableResponse.of(numTransactions, future);
        }
    }

    private static final class MultiClientRequestManager implements AutoCloseable {
        private final Map<Namespace, RequestParams> requestMap;

        private MultiClientRequestManager(Map<Namespace, RequestParams> requestMap) {
            this.requestMap = requestMap;
        }

        private boolean requestsPending() {
            return !requestMap.isEmpty();
        }

        private void updatePendingStartTransactionsCount(Namespace namespace, int startedTransactionsCount) {
            requestMap.compute(
                    namespace, (_unused, params) -> remainingRequestsForNamespace(params, startedTransactionsCount));
        }

        private RequestParams remainingRequestsForNamespace(RequestParams params, int startedTransactionsCount) {
            int numTransactions = params.numTransactions();
            if (startedTransactionsCount < numTransactions) {
                return ImmutableRequestParams.builder()
                        .from(params)
                        .numTransactions(numTransactions - startedTransactionsCount)
                        .build();
            }
            return null;
        }

        @Override
        public void close() {
            requestMap.clear();
        }
    }

    private static final class ResponseHandler implements AutoCloseable {
        private final Queue<SettableResponse> pendingFutures;
        private final Queue<StartIdentifiedAtlasDbTransactionResponse> transientResponseList;
        private final LockCleanupService lockCleanupService;

        ResponseHandler(LockCleanupService lockCleanupService) {
            this.pendingFutures = new ArrayDeque<>();
            this.transientResponseList = new ArrayDeque<>();
            this.lockCleanupService = lockCleanupService;
        }

        private void addPendingFuture(SettableResponse future) {
            pendingFutures.add(future);
        }

        private void processResponse(List<StartIdentifiedAtlasDbTransactionResponse> responses) {
            transientResponseList.addAll(responses);
            serveRequests();
        }

        private void serveRequests() {
            while (!pendingFutures.isEmpty()) {
                int numRequired = pendingFutures.peek().numTransactions();
                if (numRequired > transientResponseList.size()) {
                    break;
                }

                ImmutableList.Builder<StartIdentifiedAtlasDbTransactionResponse> builder = ImmutableList.builder();
                for (int transactionIndex = 0; transactionIndex < numRequired; transactionIndex++) {
                    builder.add(Objects.requireNonNull(transientResponseList.poll()));
                }
                pendingFutures.poll().startTransactionsFuture().set(builder.build());
            }
        }

        @Override
        public void close() {
            if (!transientResponseList.isEmpty()) {
                TransactionStarterHelper.unlock(
                        transientResponseList.stream()
                                .map(response -> response.immutableTimestamp().getLock())
                                .collect(Collectors.toSet()),
                        lockCleanupService);
            }
            pendingFutures.clear();
            transientResponseList.clear();
        }
    }
}
