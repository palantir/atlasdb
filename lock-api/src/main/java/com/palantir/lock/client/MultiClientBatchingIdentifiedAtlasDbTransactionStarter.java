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

import static com.palantir.lock.client.LockLeaseService.toConjure;

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
import com.palantir.lock.watch.LockWatchVersion;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.immutables.value.Value;

public class MultiClientBatchingIdentifiedAtlasDbTransactionStarter implements AutoCloseable {
    private final DisruptorAutobatcher<
                    NamespacedStartTransactionsRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>
            autobatcher;

    public MultiClientBatchingIdentifiedAtlasDbTransactionStarter(
            DisruptorAutobatcher<
                            NamespacedStartTransactionsRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>
                    autobatcher) {
        this.autobatcher = autobatcher;
    }

    static MultiClientBatchingIdentifiedAtlasDbTransactionStarter create(
            InternalMultiClientConjureTimelockService delegate) {
        DisruptorAutobatcher<NamespacedStartTransactionsRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>
                autobatcher = Autobatchers.independent(consumer(delegate, UUID.randomUUID()))
                        .safeLoggablePurpose("multi-client-transaction-starter")
                        .build();
        return new MultiClientBatchingIdentifiedAtlasDbTransactionStarter(autobatcher);
    }

    public List<StartIdentifiedAtlasDbTransactionResponse> startTransactions(
            Namespace namespace, int request, Supplier<Optional<LockWatchVersion>> lockWatchVersionSuppplier) {
        return AtlasFutures.getUnchecked(autobatcher.apply(NamespacedStartTransactionsRequestParams.of(
                namespace, StartTransactionsRequestParams.of(request, lockWatchVersionSuppplier))));
    }

    private static Consumer<
                    List<
                            BatchElement<
                                    NamespacedStartTransactionsRequestParams,
                                    List<StartIdentifiedAtlasDbTransactionResponse>>>>
            consumer(InternalMultiClientConjureTimelockService delegate, UUID requestorId) {
        return batch -> processBatch(delegate, requestorId, batch);
    }

    @VisibleForTesting
    static void processBatch(
            InternalMultiClientConjureTimelockService delegate,
            UUID requestorId,
            List<
                            BatchElement<
                                    NamespacedStartTransactionsRequestParams,
                                    List<StartIdentifiedAtlasDbTransactionResponse>>>
                    batch) {

        Map<Namespace, ResponseHandler> namespaceWiseResponseHandler = getNamespaceWisePendingRequests(batch);
        MultiClientRequestManager multiClientRequestManager =
                new MultiClientRequestManager(getNamespaceWiseRequestParams(batch));

        Map<Namespace, List<StartIdentifiedAtlasDbTransactionResponse>> startTransactionResponses;
        while (multiClientRequestManager.requestsPending()) {
            startTransactionResponses =
                    getStartTransactionResponses(multiClientRequestManager.requestMap, delegate, requestorId);

            for (Map.Entry<Namespace, List<StartIdentifiedAtlasDbTransactionResponse>> namespacedResponses :
                    startTransactionResponses.entrySet()) {
                Namespace namespace = namespacedResponses.getKey();
                List<StartIdentifiedAtlasDbTransactionResponse> responseList = namespacedResponses.getValue();

                multiClientRequestManager.updatePendingStartTransactionsCount(namespace, responseList.size());

                namespaceWiseResponseHandler.get(namespace).acceptResponsesAndServeRequestsGreedily(responseList);
            }
        }
    }

    private static Map<Namespace, ResponseHandler> getNamespaceWisePendingRequests(
            List<
                            BatchElement<
                                    NamespacedStartTransactionsRequestParams,
                                    List<StartIdentifiedAtlasDbTransactionResponse>>>
                    batch) {
        Map<Namespace, Queue<SettableResponse>> namespaceWisePendingFutures = new HashMap<>();

        for (BatchElement<NamespacedStartTransactionsRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>
                element : batch) {
            Namespace namespace = element.argument().namespace();
            namespaceWisePendingFutures
                    .computeIfAbsent(namespace, _u -> new LinkedList<>())
                    .add(SettableResponse.of(element.argument().params().numTransactions(), element.result()));
        }

        return KeyedStream.stream(namespaceWisePendingFutures)
                .map(ResponseHandler::new)
                .collectToMap();
    }

    private static Map<Namespace, StartTransactionsRequestParams> getNamespaceWiseRequestParams(
            List<
                            BatchElement<
                                    NamespacedStartTransactionsRequestParams,
                                    List<StartIdentifiedAtlasDbTransactionResponse>>>
                    batch) {
        return batch.stream()
                .map(batchElement -> batchElement.argument())
                .collect(Collectors.toMap(
                        NamespacedStartTransactionsRequestParams::namespace,
                        NamespacedStartTransactionsRequestParams::params,
                        StartTransactionsRequestParams::coalesce));
    }

    private static Map<Namespace, List<StartIdentifiedAtlasDbTransactionResponse>> getStartTransactionResponses(
            Map<Namespace, StartTransactionsRequestParams> originalRequestMap,
            InternalMultiClientConjureTimelockService delegate,
            UUID requestorId) {

        Map<Namespace, ConjureStartTransactionsRequest> namespaceWiseRequests = KeyedStream.stream(originalRequestMap)
                .mapEntries((namespace, requestParams) ->
                        Maps.immutableEntry(namespace, getConjureRequest(requestParams, requestorId)))
                .collectToMap();

        Map<Namespace, ConjureStartTransactionsResponse> responseMap = getResponseMap(delegate, namespaceWiseRequests);

        return KeyedStream.stream(responseMap)
                .map(TransactionStarterHelper::split)
                .collectToMap();
    }

    private static ConjureStartTransactionsRequest getConjureRequest(
            StartTransactionsRequestParams requestParams, UUID requestorId) {
        return ConjureStartTransactionsRequest.builder()
                .requestorId(requestorId)
                .requestId(UUID.randomUUID())
                .numTransactions(requestParams.numTransactions())
                .lastKnownVersion(
                        toConjure(requestParams.lockWatchVersionSupplier().get()))
                .build();
    }

    private static Map<Namespace, ConjureStartTransactionsResponse> getResponseMap(
            InternalMultiClientConjureTimelockService delegate,
            Map<Namespace, ConjureStartTransactionsRequest> namespaceWiseRequests) {
        return KeyedStream.stream(delegate.startTransactions(namespaceWiseRequests))
                .map(LockLeaseService::getMassagedConjureStartTransactionsResponse)
                .collectToMap();
    }

    @Override
    public void close() {
        autobatcher.close();
    }

    @Value.Immutable
    interface NamespacedStartTransactionsRequestParams {
        @Value.Parameter
        Namespace namespace();

        @Value.Parameter
        StartTransactionsRequestParams params();

        static NamespacedStartTransactionsRequestParams of(Namespace namespace, StartTransactionsRequestParams params) {
            return ImmutableNamespacedStartTransactionsRequestParams.of(namespace, params);
        }
    }

    @Value.Immutable
    interface StartTransactionsRequestParams {
        @Value.Parameter
        Integer numTransactions();

        @Value.Parameter
        Supplier<Optional<LockWatchVersion>> lockWatchVersionSupplier();

        static StartTransactionsRequestParams of(
                int numTransactions, Supplier<Optional<LockWatchVersion>> lockWatchVersion) {
            return ImmutableStartTransactionsRequestParams.of(numTransactions, lockWatchVersion);
        }

        static StartTransactionsRequestParams coalesce(
                StartTransactionsRequestParams params1, StartTransactionsRequestParams params2) {
            return StartTransactionsRequestParams.of(
                    params1.numTransactions() + params2.numTransactions(), params1.lockWatchVersionSupplier());
        }
    }

    @Value.Immutable
    interface SettableResponse {
        @Value.Parameter
        Integer numTransactions();

        @Value.Parameter
        DisruptorFuture<List<StartIdentifiedAtlasDbTransactionResponse>> future();

        static SettableResponse of(
                int numTransactions, DisruptorFuture<List<StartIdentifiedAtlasDbTransactionResponse>> future) {
            return ImmutableSettableResponse.of(numTransactions, future);
        }
    }

    private static class MultiClientRequestManager {
        private final Map<Namespace, StartTransactionsRequestParams> requestMap;

        public MultiClientRequestManager(Map<Namespace, StartTransactionsRequestParams> requestMap) {
            this.requestMap = requestMap;
        }

        public boolean requestsPending() {
            return !requestMap.isEmpty();
        }

        public void updatePendingStartTransactionsCount(Namespace namespace, int startedTransactionsCount) {
            StartTransactionsRequestParams updatedParams = paramsAfterResponse(namespace, startedTransactionsCount);
            if (updatedParams == null) {
                requestMap.remove(namespace);
            } else {
                requestMap.put(namespace, updatedParams);
            }
        }

        private StartTransactionsRequestParams paramsAfterResponse(Namespace namespace, int startedTransactionsCount) {
            StartTransactionsRequestParams params = requestMap.get(namespace);
            int numTransactions = params.numTransactions();
            return startedTransactionsCount < numTransactions
                    ? StartTransactionsRequestParams.of(
                            numTransactions - startedTransactionsCount, params.lockWatchVersionSupplier())
                    : null;
        }
    }

    private static class ResponseHandler {
        private final Queue<SettableResponse> pendingRequestQueue;
        private List<StartIdentifiedAtlasDbTransactionResponse> responseList;
        private int start;

        ResponseHandler(Queue<SettableResponse> pendingRequestQueue) {
            this.pendingRequestQueue = pendingRequestQueue;
            this.responseList = new ArrayList<>();
            this.start = 0;
        }

        public void acceptResponsesAndServeRequestsGreedily(List<StartIdentifiedAtlasDbTransactionResponse> responses) {
            acceptResponses(responses);
            serveRequests();
        }

        private void acceptResponses(List<StartIdentifiedAtlasDbTransactionResponse> responses) {
            if (responses.size() < responseList.size()) {
                responseList.addAll(responses);
            } else {
                responses.addAll(responseList);
                responseList = responses;
            }
        }

        private void serveRequests() {
            int end;
            while (!pendingRequestQueue.isEmpty()) {
                int numRequired = pendingRequestQueue.peek().numTransactions();
                if (start + numRequired > responseList.size()) {
                    break;
                }
                end = start + numRequired;
                pendingRequestQueue.poll().future().set(ImmutableList.copyOf(responseList.subList(start, end)));
                start = end;
            }
            reset();
        }

        private void reset() {
            responseList = responseList.subList(start, responseList.size());
            start = 0;
        }
    }
}
