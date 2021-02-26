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

import static com.palantir.lock.client.MultiClientTransactionStarter.processBatch;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher.DisruptorFuture;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.client.MultiClientTransactionStarter.NamespaceAndRequestParams;
import com.palantir.lock.client.MultiClientTransactionStarter.RequestParams;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.watch.StartTransactionsLockWatchEventCache;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class MultiClientTransactionStarterTest {
    private static final int PARTITIONED_TIMESTAMPS_LIMIT_PER_SERVER_CALL = 5;
    private final InternalMultiClientConjureTimelockService timelockService =
            mock(InternalMultiClientConjureTimelockService.class);
    private final LockCleanupService lockCleanupService = mock(LockCleanupService.class);
    private static final Map<Namespace, StartTransactionsLockWatchEventCache> NAMESPACE_CACHE_MAP = new HashMap();
    private int lowestStartTs = 1;

    @Test
    public void canServiceOneClient() {
        setupServiceAndAssertSanity(
                getStartTransactionRequestsForClients(1, PARTITIONED_TIMESTAMPS_LIMIT_PER_SERVER_CALL - 1));
    }

    @Test
    public void canServiceOneClientWithMultipleServerCalls() {
        setupServiceAndAssertSanity(
                getStartTransactionRequestsForClients(1, PARTITIONED_TIMESTAMPS_LIMIT_PER_SERVER_CALL * 27));
    }

    @Test
    public void canServiceMultipleClients() {
        int clientCount = 50;
        setupServiceAndAssertSanity(getStartTransactionRequestsForClients(
                clientCount, (PARTITIONED_TIMESTAMPS_LIMIT_PER_SERVER_CALL - 1) * clientCount));
    }

    @Test
    public void canServiceMultipleClientsWithMultipleServerCalls() {
        int clientCount = 5;
        setupServiceAndAssertSanity(getStartTransactionRequestsForClients(
                clientCount, (PARTITIONED_TIMESTAMPS_LIMIT_PER_SERVER_CALL + 1) * clientCount));
    }

    @Test
    public void canServiceOneRequestWithMultipleServerRequests() {
        Namespace namespace = Namespace.of("Test_0");
        setupServiceAndAssertSanity(ImmutableList.of(batchElementForNamespace(namespace, 127)));
    }

    @Test
    public void updatesCacheWhileProcessingResponse() {
        Namespace namespace = Namespace.of("Test" + UUID.randomUUID());
        setupServiceAndAssertSanity(ImmutableList.of(
                batchElementForNamespace(namespace, PARTITIONED_TIMESTAMPS_LIMIT_PER_SERVER_CALL - 1)));
        verify(getCache(namespace)).processStartTransactionsUpdate(any(), any());
    }

    @Test
    public void shouldFreeResourcesIfServerThrows() {
        Namespace namespace = Namespace.of("Test" + UUID.randomUUID());
        UUID requestorId = UUID.randomUUID();

        BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>> requestToBeServed =
                batchElementForNamespace(namespace, 1);
        BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>> requestNotToBeServed =
                batchElementForNamespace(namespace, PARTITIONED_TIMESTAMPS_LIMIT_PER_SERVER_CALL * 5);

        ImmutableList<BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>>
                requests = ImmutableList.of(
                requestToBeServed,
                requestNotToBeServed);
        Map<Namespace, ConjureStartTransactionsResponse> responseMap = startTransactionsResponse(requests, requestorId);

        SafeIllegalStateException exception = new SafeIllegalStateException("Something went wrong!");
        when(timelockService.startTransactions(any())).thenReturn(responseMap).thenThrow(exception);

        assertThatThrownBy(() -> processBatch(timelockService, requestorId, requests))
                .isEqualTo(exception);

        assertSanityOfRequestBatch(
                ImmutableList.of(requestToBeServed),
                ImmutableMap.of(namespace, ImmutableList.of(responseMap.get(namespace))));
        assertThat(requestNotToBeServed.result().isDone()).isFalse();

        verify(lockCleanupService).refreshLockLeases(any());
        verify(lockCleanupService).unlock(any());
    }

    @Test
    public void shouldNotFreeResourcesIfRequestIsServed() {
        Namespace alpha = Namespace.of("alpha" + UUID.randomUUID());
        Namespace beta = Namespace.of("beta" + UUID.randomUUID());

        BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>> requestForAlpha =
                batchElementForNamespace(alpha, PARTITIONED_TIMESTAMPS_LIMIT_PER_SERVER_CALL - 1);
        BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>> requestForBeta =
                batchElementForNamespace(beta, PARTITIONED_TIMESTAMPS_LIMIT_PER_SERVER_CALL * 5);

        UUID requestorId = UUID.randomUUID();

        List<BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>> requests =
                ImmutableList.of(requestForAlpha, requestForBeta);

        Map<Namespace, ConjureStartTransactionsResponse> responseMap = startTransactionsResponse(requests, requestorId);

        SafeIllegalStateException exception = new SafeIllegalStateException("Something went wrong!");
        when(timelockService.startTransactions(any())).thenReturn(responseMap).thenThrow(exception);

        assertThatThrownBy(() -> processBatch(timelockService, requestorId, requests))
                .isEqualTo(exception);

        // assert requests made by client alpha are served
        assertSanityOfRequestBatch(
                ImmutableList.of(requestForAlpha), ImmutableMap.of(alpha, ImmutableList.of(responseMap.get(alpha))));

        // assert clean up was done by exactly one service - beta
        verify(lockCleanupService).refreshLockLeases(any());
        verify(lockCleanupService).unlock(any());
    }

    private BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>
            batchElementForNamespace(Namespace beta, int numTransactions) {
        return BatchElement.of(
                NamespaceAndRequestParams.of(
                        beta, RequestParams.of(numTransactions, getCache(beta), lockCleanupService)),
                new DisruptorFuture<>("test"));
    }

    private void setupServiceAndAssertSanity(
            List<BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>>
                    requestsForClients) {

        UUID requestorId = UUID.randomUUID();
        Map<Namespace, List<ConjureStartTransactionsResponse>> responseMap = new HashMap<>();
        when(timelockService.startTransactions(any())).thenAnswer(invocation -> {
            Map<Namespace, ConjureStartTransactionsResponse> responses =
                    startTransactions(invocation.getArgument(0), getLowestTs());
            responses.forEach((namespace, response) -> {
                responseMap.computeIfAbsent(namespace, _u -> new ArrayList()).add(response);
            });
            return responses;
        });

        processBatch(timelockService, requestorId, requestsForClients);
        // assertions on responses
        assertSanityOfRequestBatch(requestsForClients, responseMap);
    }

    private void assertSanityOfRequestBatch(
            List<BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>>
                    requestsForClients,
            Map<Namespace, List<ConjureStartTransactionsResponse>> responseMap) {

        requestsForClients.forEach(batchElement -> {
            DisruptorFuture<List<StartIdentifiedAtlasDbTransactionResponse>> resultFuture = batchElement.result();
            NamespaceAndRequestParams requestParams = batchElement.argument();
            assertThat(resultFuture.isDone()).isTrue();

            List<StartIdentifiedAtlasDbTransactionResponse> responseList = Futures.getUnchecked(resultFuture);
            assertThat(responseList).hasSize(requestParams.params().numTransactions());
        });

        Map<Namespace, List<BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>>>
                partitionedResponses = requestsForClients.stream()
                        .collect(Collectors.groupingBy(e -> e.argument().namespace()));

        responseMap.forEach((namespace, responses) -> {
            int startInd = 0;
            List<StartIdentifiedAtlasDbTransactionResponse> startedTransactions =
                    partitionedResponses.get(namespace).stream()
                            .map(response -> Futures.getUnchecked(response.result()))
                            .flatMap(Collection::stream)
                            .collect(Collectors.toList());

            Iterator<ConjureStartTransactionsResponse> responseIterator = responses.iterator();

            while (responseIterator.hasNext()) {
                ConjureStartTransactionsResponse conjureResponse = responseIterator.next();
                List<StartIdentifiedAtlasDbTransactionResponse> responseList;

                int toIndex =
                        Math.min(startInd + conjureResponse.getTimestamps().count(), startedTransactions.size());

                responseList = startedTransactions.subList(startInd, toIndex);
                startInd = toIndex;

                assertThat(responseList)
                        .satisfies(StartTransactionsTestUtils::assertThatStartTransactionResponsesAreUnique)
                        .allSatisfy(startTxnResponse -> StartTransactionsTestUtils.assertDerivableFromBatchedResponse(
                                startTxnResponse, conjureResponse));
            }
        });
    }

    private int getLowestTs() {
        return lowestStartTs += PARTITIONED_TIMESTAMPS_LIMIT_PER_SERVER_CALL;
    }

    private Map<Namespace, ConjureStartTransactionsResponse> startTransactionsResponse(
            List<BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>>
                    requestsForClients,
            UUID requestorId) {
        Map<Namespace, RequestParams> requestParams =
                MultiClientTransactionStarter.partitionRequestsByNamespace(requestsForClients);
        Map<Namespace, ConjureStartTransactionsRequest> conjureRequests =
                MultiClientTransactionStarter.getConjureRequests(requestParams, requestorId);
        return startTransactions(conjureRequests, 1);
    }

    private List<BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>>
            getStartTransactionRequestsForClients(int clientCount, int requestCount) {
        return IntStream.rangeClosed(1, requestCount)
                .mapToObj(ind -> {
                    Namespace namespace = Namespace.of("Test_" + (ind % clientCount));
                    return batchElementForNamespace(namespace, 1);
                })
                .collect(Collectors.toList());
    }

    private StartTransactionsLockWatchEventCache getCache(Namespace namespace) {
        return NAMESPACE_CACHE_MAP.computeIfAbsent(namespace, _u -> mock(StartTransactionsLockWatchEventCache.class));
    }

    public Map<Namespace, ConjureStartTransactionsResponse> startTransactions(
            Map<Namespace, ConjureStartTransactionsRequest> requests, int lowestStartTs) {
        return KeyedStream.stream(requests)
                .map(request -> StartTransactionsTestUtils.getStartTransactionResponse(
                        lowestStartTs,
                        Math.min(PARTITIONED_TIMESTAMPS_LIMIT_PER_SERVER_CALL, request.getNumTransactions())))
                .collectToMap();
    }
}
