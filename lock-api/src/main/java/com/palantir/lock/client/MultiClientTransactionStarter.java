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
import com.palantir.lock.watch.StartTransactionsLockWatchEventCache;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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

    static MultiClientTransactionStarter create(InternalMultiClientConjureTimelockService delegate) {
        DisruptorAutobatcher<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>> autobatcher =
                Autobatchers.independent(consumer(delegate, UUID.randomUUID()))
                        .safeLoggablePurpose("multi-client-transaction-starter")
                        .build();
        return new MultiClientTransactionStarter(autobatcher);
    }

    public List<StartIdentifiedAtlasDbTransactionResponse> startTransactions(
            Namespace namespace,
            int request,
            StartTransactionsLockWatchEventCache cache,
            LockCleanupService lockCleanupService) {
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

        Map<Namespace, ResponseHandler> namespaceWiseResponseHandler = getResponseHandlers(batch);
        MultiClientRequestManager multiClientRequestManager =
                new MultiClientRequestManager(getNamespaceWiseRequestParams(batch));

        Map<Namespace, List<StartIdentifiedAtlasDbTransactionResponse>> startTransactionResponses = new HashMap<>();

        try {
            while (multiClientRequestManager.requestsPending()) {
                startTransactionResponses =
                        getStartTransactionResponses(multiClientRequestManager.requestMap, delegate, requestorId);
                startTransactionResponses.forEach((namespace, responseList) -> {
                    multiClientRequestManager.updatePendingStartTransactionsCount(namespace, responseList.size());
                    namespaceWiseResponseHandler.get(namespace).processResponse(responseList);
                });
            }
        } finally {
            clearResources(multiClientRequestManager, namespaceWiseResponseHandler);
        }
    }

    private static void clearResources(
            MultiClientRequestManager multiClientRequestManager,
            Map<Namespace, ResponseHandler> namespaceWiseResponseHandler) {
        multiClientRequestManager.close();
        namespaceWiseResponseHandler.forEach((_unused, responseHandler) -> responseHandler.close());
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
    static Map<Namespace, RequestParams> getNamespaceWiseRequestParams(
            List<BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>> batch) {
        return batch.stream()
                .map(BatchElement::argument)
                .collect(Collectors.toMap(
                        NamespaceAndRequestParams::namespace, NamespaceAndRequestParams::params, RequestParams::merge));
    }

    private static Map<Namespace, List<StartIdentifiedAtlasDbTransactionResponse>> getStartTransactionResponses(
            Map<Namespace, RequestParams> originalRequestMap,
            InternalMultiClientConjureTimelockService delegate,
            UUID requestorId) {

        Map<Namespace, ConjureStartTransactionsRequest> namespaceWiseRequests =
                getNamespaceWiseRequests(originalRequestMap, requestorId);
        Map<Namespace, ConjureStartTransactionsResponse> responseMap = getResponseMap(delegate, namespaceWiseRequests);
        return KeyedStream.stream(responseMap)
                .mapEntries((namespace, response) -> {
                    TransactionStarterHelper.updateCacheWithStartTransactionResponse(
                            originalRequestMap.get(namespace).cache(),
                            fromConjure(namespaceWiseRequests.get(namespace).getLastKnownVersion()),
                            response);
                    return Maps.immutableEntry(namespace, TransactionStarterHelper.split(response));
                })
                .collectToMap();
    }

    @VisibleForTesting
    static Map<Namespace, ConjureStartTransactionsRequest> getNamespaceWiseRequests(
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
                .lastKnownVersion(toConjure(requestParams.cache().lastKnownVersion()))
                .build();
    }

    private static Map<Namespace, ConjureStartTransactionsResponse> getResponseMap(
            InternalMultiClientConjureTimelockService delegate,
            Map<Namespace, ConjureStartTransactionsRequest> namespaceWiseRequests) {
        return KeyedStream.stream(delegate.startTransactions(namespaceWiseRequests))
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
        StartTransactionsLockWatchEventCache cache();

        @Value.Parameter
        LockCleanupService lockCleanupService();

        static RequestParams of(
                int numTransactions,
                StartTransactionsLockWatchEventCache cache,
                LockCleanupService lockCleanupService) {
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

    private static class MultiClientRequestManager implements AutoCloseable {
        private final Map<Namespace, RequestParams> requestMap;

        public MultiClientRequestManager(Map<Namespace, RequestParams> requestMap) {
            this.requestMap = requestMap;
        }

        public boolean requestsPending() {
            return !requestMap.isEmpty();
        }

        public void updatePendingStartTransactionsCount(Namespace namespace, int startedTransactionsCount) {
            Optional<RequestParams> updatedParams = paramsAfterResponse(namespace, startedTransactionsCount);
            if (updatedParams.isPresent()) {
                requestMap.put(namespace, updatedParams.get());
            } else {
                requestMap.remove(namespace);
            }
        }

        private Optional<RequestParams> paramsAfterResponse(Namespace namespace, int startedTransactionsCount) {
            RequestParams params = requestMap.get(namespace);
            int numTransactions = params.numTransactions();
            return startedTransactionsCount < numTransactions
                    ? Optional.of(RequestParams.of(
                            numTransactions - startedTransactionsCount, params.cache(), params.lockCleanupService()))
                    : Optional.empty();
        }

        @Override
        public void close() {
            requestMap.clear();
        }
    }

    private static class ResponseHandler implements AutoCloseable {
        private final Queue<SettableResponse> pendingFutures;
        private Queue<StartIdentifiedAtlasDbTransactionResponse> transientResponseList;
        private final LockCleanupService lockCleanupService;

        ResponseHandler(LockCleanupService lockCleanupService) {
            this.pendingFutures = new LinkedList<>();
            this.transientResponseList = new LinkedList<>();
            this.lockCleanupService = lockCleanupService;
        }

        public void addPendingFuture(SettableResponse future) {
            pendingFutures.add(future);
        }

        public void processResponse(List<StartIdentifiedAtlasDbTransactionResponse> responses) {
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
            TransactionStarterHelper.unlock(
                    transientResponseList.stream()
                            .map(response -> response.immutableTimestamp().getLock())
                            .collect(Collectors.toSet()),
                    lockCleanupService);
            pendingFutures.clear();
            transientResponseList.clear();
        }
    }
}
