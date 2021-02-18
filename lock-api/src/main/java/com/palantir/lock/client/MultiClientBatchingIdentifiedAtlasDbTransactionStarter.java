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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.immutables.value.Value;

public class MultiClientBatchingIdentifiedAtlasDbTransactionStarter implements AutoCloseable {
    private final DisruptorAutobatcher<
                    Map.Entry<Namespace, StartTransactionsRequestParams>,
                    List<StartIdentifiedAtlasDbTransactionResponse>>
            autobatcher;

    public MultiClientBatchingIdentifiedAtlasDbTransactionStarter(
            DisruptorAutobatcher<
                            Map.Entry<Namespace, StartTransactionsRequestParams>,
                            List<StartIdentifiedAtlasDbTransactionResponse>>
                    autobatcher) {
        this.autobatcher = autobatcher;
    }

    static MultiClientBatchingIdentifiedAtlasDbTransactionStarter create(
            InternalMultiClientConjureTimelockService delegate) {
        DisruptorAutobatcher<
                        Map.Entry<Namespace, StartTransactionsRequestParams>,
                        List<StartIdentifiedAtlasDbTransactionResponse>>
                autobatcher = Autobatchers.independent(consumer(delegate, UUID.randomUUID()))
                        .safeLoggablePurpose("multi-client-transaction-starter")
                        .build();
        return new MultiClientBatchingIdentifiedAtlasDbTransactionStarter(autobatcher);
    }

    public List<StartIdentifiedAtlasDbTransactionResponse> startTransactions(
            Namespace namespace, int request, Supplier<Optional<LockWatchVersion>> lockWatchVersionSuppplier) {
        return AtlasFutures.getUnchecked(autobatcher.apply(
                Maps.immutableEntry(namespace, StartTransactionsRequestParams.of(request, lockWatchVersionSuppplier))));
    }

    private static Consumer<
                    List<
                            BatchElement<
                                    Map.Entry<Namespace, StartTransactionsRequestParams>,
                                    List<StartIdentifiedAtlasDbTransactionResponse>>>>
            consumer(InternalMultiClientConjureTimelockService delegate, UUID requestorId) {
        return batch -> serviceAllRequests(delegate, requestorId, batch);
    }

    private static void serviceAllRequests(
            InternalMultiClientConjureTimelockService delegate,
            UUID requestorId,
            List<
                            BatchElement<
                                    Entry<Namespace, StartTransactionsRequestParams>,
                                    List<StartIdentifiedAtlasDbTransactionResponse>>>
                    batch) {

        List<
                        BatchElement<
                                Entry<Namespace, StartTransactionsRequestParams>,
                                List<StartIdentifiedAtlasDbTransactionResponse>>>
                pendingRequests = new ArrayList<>(batch);

        while (!pendingRequests.isEmpty()) {
            Map<Namespace, StartTransactionsRequestParams> namespaceWiseRequestParams = pendingRequests.stream()
                    .map(batchElement -> batchElement.argument())
                    .collect(
                            Collectors.toMap(Entry::getKey, Entry::getValue, StartTransactionsRequestParams::coalesce));

            Map<Namespace, List<StartIdentifiedAtlasDbTransactionResponse>> result =
                    getStartTransactionResponses(namespaceWiseRequestParams, delegate, requestorId);

            Map<Namespace, Integer> responseTracker = new HashMap<>();
            for (BatchElement<
                            Entry<Namespace, StartTransactionsRequestParams>,
                            List<StartIdentifiedAtlasDbTransactionResponse>>
                    batchElement : pendingRequests) {
                Entry<Namespace, StartTransactionsRequestParams> argument = batchElement.argument();
                Namespace namespace = argument.getKey();
                int start = responseTracker.putIfAbsent(namespace, 0);

                if (start > result.get(namespace).size()) {
                    continue;
                }

                int end = start + argument.getValue().numTransactions();
                batchElement
                        .result()
                        .set(ImmutableList.copyOf(result.get(namespace).subList(start, end)));
                responseTracker.put(namespace, end);
            }

            pendingRequests =
                    pendingRequests.stream().filter(x -> !x.result().isDone()).collect(Collectors.toList());
        }
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
