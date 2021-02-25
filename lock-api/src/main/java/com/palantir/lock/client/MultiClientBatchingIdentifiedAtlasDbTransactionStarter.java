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
import com.palantir.common.base.Throwables;
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

public final class MultiClientBatchingIdentifiedAtlasDbTransactionStarter implements AutoCloseable {
    private final DisruptorAutobatcher<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>
            autobatcher;

    private MultiClientBatchingIdentifiedAtlasDbTransactionStarter(
            DisruptorAutobatcher<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>
                    autobatcher) {
        this.autobatcher = autobatcher;
    }

    static MultiClientBatchingIdentifiedAtlasDbTransactionStarter create(
            InternalMultiClientConjureTimelockService delegate) {
        DisruptorAutobatcher<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>> autobatcher =
                Autobatchers.independent(consumer(delegate, UUID.randomUUID()))
                        .safeLoggablePurpose("multi-client-transaction-starter")
                        .build();
        return new MultiClientBatchingIdentifiedAtlasDbTransactionStarter(autobatcher);
    }

    public List<StartIdentifiedAtlasDbTransactionResponse> startTransactions(
            Namespace namespace,
            int request,
            StartTransactionsLockWatchEventCache cache,
            LockLeaseService.LockCleanupService lockCleanupService) {
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

        while (multiClientRequestManager.requestsPending()) {
            try {
                startTransactionResponses =
                        getStartTransactionResponses(multiClientRequestManager.requestMap, delegate, requestorId);
                startTransactionResponses.forEach((namespace, responseList) -> {
                    multiClientRequestManager.updatePendingStartTransactionsCount(namespace, responseList.size());
                    namespaceWiseResponseHandler.get(namespace).processResponse(responseList);
                });
            } catch (Throwable t) {
                clearResources(multiClientRequestManager, startTransactionResponses);
                throw Throwables.throwUncheckedException(t);
            }
        }
    }

    private static void clearResources(
            MultiClientRequestManager multiClientRequestManager,
            Map<Namespace, List<StartIdentifiedAtlasDbTransactionResponse>> startTransactionResponses) {
        startTransactionResponses.forEach((namespace, responseList) -> TransactionStarterHelper.unlock(
                responseList.stream()
                        .map(response -> response.immutableTimestamp().getLock())
                        .collect(Collectors.toSet()),
                multiClientRequestManager.getLockCleanupService(namespace)));
    }

    private static Map<Namespace, ResponseHandler> getResponseHandlers(
            List<BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>> batch) {
        Map<Namespace, Queue<SettableResponse>> namespaceWisePendingFutures = new HashMap<>();

        for (BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>> element : batch) {
            Namespace namespace = element.argument().namespace();
            namespaceWisePendingFutures
                    .computeIfAbsent(namespace, _u -> new LinkedList<>())
                    .add(SettableResponse.of(element.argument().params().numTransactions(), element.result()));
        }

        return KeyedStream.stream(namespaceWisePendingFutures)
                .map(ResponseHandler::new)
                .collectToMap();
    }

    @VisibleForTesting
    static Map<Namespace, RequestParams> getNamespaceWiseRequestParams(
            List<BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>> batch) {
        return batch.stream()
                .map(BatchElement::argument)
                .collect(Collectors.toMap(
                        NamespaceAndRequestParams::namespace,
                        NamespaceAndRequestParams::params,
                        RequestParams::coalesce));
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
                    originalRequestMap
                            .get(namespace)
                            .cache()
                            .processStartTransactionsUpdate(
                                    response.getTimestamps().stream().boxed().collect(Collectors.toSet()),
                                    response.getLockWatchUpdate());
                    LockWatchLogUtility.logTransactionEvents(
                            fromConjure(namespaceWiseRequests.get(namespace).getLastKnownVersion()),
                            response.getLockWatchUpdate());
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
        LockLeaseService.LockCleanupService lockCleanupService();

        static RequestParams of(
                int numTransactions,
                StartTransactionsLockWatchEventCache cache,
                LockLeaseService.LockCleanupService lockCleanupService) {
            return ImmutableRequestParams.of(numTransactions, cache, lockCleanupService);
        }

        static RequestParams coalesce(RequestParams params1, RequestParams params2) {
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

    private static class MultiClientRequestManager {
        private final Map<Namespace, RequestParams> requestMap;

        public MultiClientRequestManager(Map<Namespace, RequestParams> requestMap) {
            this.requestMap = requestMap;
        }

        public boolean requestsPending() {
            return !requestMap.isEmpty();
        }

        public void updatePendingStartTransactionsCount(Namespace namespace, int startedTransactionsCount) {
            Optional<RequestParams> updatedParams = paramsAfterResponse(namespace, startedTransactionsCount);
            if (!updatedParams.isPresent()) {
                requestMap.remove(namespace);
            } else {
                requestMap.put(namespace, updatedParams.get());
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

        // Maybe this should be somewhere else?
        public LockLeaseService.LockCleanupService getLockCleanupService(Namespace namespace) {
            return requestMap.get(namespace).lockCleanupService();
        }
    }

    private static class ResponseHandler {
        private final Queue<SettableResponse> pendingRequestQueue;
        private Queue<StartIdentifiedAtlasDbTransactionResponse> pendingResponseQueue;

        ResponseHandler(Queue<SettableResponse> pendingRequestQueue) {
            this.pendingRequestQueue = pendingRequestQueue;
            this.pendingResponseQueue = new LinkedList<>();
        }

        public void processResponse(List<StartIdentifiedAtlasDbTransactionResponse> responses) {
            pendingResponseQueue.addAll(responses);
            serveRequests();
        }

        private void serveRequests() {
            while (!pendingRequestQueue.isEmpty()) {
                int numRequired = pendingRequestQueue.peek().numTransactions();
                if (numRequired > pendingResponseQueue.size()) {
                    break;
                }

                ImmutableList.Builder<StartIdentifiedAtlasDbTransactionResponse> builder = ImmutableList.builder();
                for (int transactionIndex = 0; transactionIndex < numRequired; transactionIndex++) {
                    builder.add(Objects.requireNonNull(pendingResponseQueue.poll()));
                }
                pendingRequestQueue.poll().startTransactionsFuture().set(builder.build());
            }
        }
    }
}
