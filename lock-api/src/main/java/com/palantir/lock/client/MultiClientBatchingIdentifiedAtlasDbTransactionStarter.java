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
import static java.util.stream.Collectors.toCollection;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.watch.LockWatchVersion;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
        return batch -> {

            // todo - do the following two in one iteration on the batch
            // Map of namespace to pending requests

            Map<
                            Namespace,
                            Queue<
                                    BatchElement<
                                            NamespacedStartTransactionsRequestParams,
                                            List<StartIdentifiedAtlasDbTransactionResponse>>>>
                    namespaceWisePendingRequests = batch.stream()
                            .collect(Collectors.groupingBy(
                                    elem -> elem.argument().namespace(), toCollection(LinkedList::new)));

            // Map of namespace to pending requests params
            Map<Namespace, StartTransactionsRequestParams> namespaceWiseRequestParams =
                    getNamespaceWiseRequestParams(batch);

            Map<Namespace, List<StartIdentifiedAtlasDbTransactionResponse>> transientResult = new HashMap<>();
            Map<Namespace, Integer> progressTracker = new HashMap<>();

            while (!namespaceWiseRequestParams.isEmpty()) {
                Map<Namespace, List<StartIdentifiedAtlasDbTransactionResponse>> startTransactionResponses =
                        getStartTransactionResponses(namespaceWiseRequestParams, delegate, requestorId);
                // update pending request state
                namespaceWiseRequestParams =
                        getUpdatedPendingRequestsMap(namespaceWiseRequestParams, startTransactionResponses);

                // responses received
                transientResult = getAllResponses(transientResult, startTransactionResponses);

                // set futures where can
                KeyedStream.stream(transientResult).forEach((namespace, responseList) -> {
                    Queue<
                                    BatchElement<
                                            NamespacedStartTransactionsRequestParams,
                                            List<StartIdentifiedAtlasDbTransactionResponse>>>
                            pendings = namespaceWisePendingRequests.get(namespace);

                    int start = progressTracker.getOrDefault(namespace, 0);
                    int end = start;
                    while (!pendings.isEmpty()) {
                        int numRequired = pendings.peek().argument().params().numTransactions();
                        if (start + numRequired > responseList.size()) {
                            break;
                        }
                        end = start + numRequired; // // Todo Ahh clean this
                        BatchElement<
                                        NamespacedStartTransactionsRequestParams,
                                        List<StartIdentifiedAtlasDbTransactionResponse>>
                                batchElement = pendings.poll();
                        batchElement.result().set(ImmutableList.copyOf(responseList.subList(start, end)));
                    }
                    progressTracker.put(namespace, end);
                    // transientResult.put(namespace, responseList.subList(end, responseList.size())); // Todo is this
                    // allowed
                });
            }
        };
    }

    private static Map<Namespace, List<StartIdentifiedAtlasDbTransactionResponse>> getAllResponses(
            Map<Namespace, List<StartIdentifiedAtlasDbTransactionResponse>> transientResult,
            Map<Namespace, List<StartIdentifiedAtlasDbTransactionResponse>> startTransactionResponses) {
        return Stream.concat(transientResult.entrySet().stream(), startTransactionResponses.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (l1, l2) -> {
                    if (l1.size() > l2.size()) {
                        l1.addAll(l2);
                        return l1;
                    }
                    l2.addAll(l1);
                    return l2;
                }));
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

    private static Map<Namespace, StartTransactionsRequestParams> getUpdatedPendingRequestsMap(
            Map<Namespace, StartTransactionsRequestParams> namespaceWiseRequestParams,
            Map<Namespace, List<StartIdentifiedAtlasDbTransactionResponse>> startTransactionResponses) {
        return KeyedStream.stream(startTransactionResponses)
                .map(List::size)
                .mapEntries((namespace, size) -> {
                    StartTransactionsRequestParams params = namespaceWiseRequestParams.get(namespace);
                    int numTransactions = params.numTransactions();
                    StartTransactionsRequestParams updatedParams = size < numTransactions
                            ? StartTransactionsRequestParams.of(
                                    numTransactions - size, params.lockWatchVersionSupplier())
                            : null;
                    return Maps.immutableEntry(namespace, updatedParams);
                })
                .filter(Objects::nonNull)
                .collectToMap();
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
}
