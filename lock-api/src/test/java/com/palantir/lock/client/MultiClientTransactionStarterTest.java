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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
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
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.watch.LockWatchCache;
import com.palantir.lock.watch.LockWatchCacheImpl;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class MultiClientTransactionStarterTest {
    private static final int PARTITIONED_TIMESTAMPS_LIMIT_PER_SERVER_CALL = 5;
    private static final Map<Namespace, LockWatchCache> NAMESPACE_CACHE_MAP = new HashMap<>();
    private static final Map<Namespace, LockCleanupService> LOCK_CLEANUP_SERVICE_MAP = new HashMap<>();
    private static final SafeIllegalStateException EXCEPTION = new SafeIllegalStateException("Something went wrong!");

    private final InternalMultiClientConjureTimelockService timelockService =
            mock(InternalMultiClientConjureTimelockService.class);

    private int lowestStartTs = 1;

    @Test
    public void canServiceOneClient() {
        setupServiceAndAssertSanity(
                getStartTransactionRequestsForClients(1, PARTITIONED_TIMESTAMPS_LIMIT_PER_SERVER_CALL - 1));
    }

    @Test
    public void canServiceOneClientMakingMultipleRequests() {
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
    public void servesRequestsAsSoonAsResponseIsReceived() {
        Namespace namespace = Namespace.of("Test" + UUID.randomUUID());
        UUID requestorId = UUID.randomUUID();

        BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>> requestToBeServed =
                batchElementForNamespace(namespace, 1);
        BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>> requestNotToBeServed =
                batchElementForNamespace(namespace, PARTITIONED_TIMESTAMPS_LIMIT_PER_SERVER_CALL * 5);

        ImmutableList<BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>>
                requests = ImmutableList.of(requestToBeServed, requestNotToBeServed);
        Map<Namespace, ConjureStartTransactionsResponse> responseMap = startTransactionsResponse(requests, requestorId);

        when(timelockService.startTransactions(any())).thenReturn(responseMap).thenThrow(EXCEPTION);

        assertThatThrownBy(() -> processBatch(timelockService, requestorId, requests))
                .isEqualTo(EXCEPTION);

        // assert first request is served even if server throws on next request
        assertSanityOfRequestBatch(
                ImmutableList.of(requestToBeServed),
                ImmutableMap.of(namespace, ImmutableList.of(responseMap.get(namespace))));
        assertThat(requestNotToBeServed.result().isDone()).isFalse();

        LockCleanupService relevantLockCleanupService = LOCK_CLEANUP_SERVICE_MAP.get(namespace);
        verify(relevantLockCleanupService).refreshLockLeases(any());
        verify(relevantLockCleanupService).unlock(any());
        verify(NAMESPACE_CACHE_MAP.get(namespace), times(4)).removeTransactionStateFromCache(anyLong());
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

        when(timelockService.startTransactions(any())).thenReturn(responseMap).thenThrow(EXCEPTION);

        assertThatThrownBy(() -> processBatch(timelockService, requestorId, requests))
                .isEqualTo(EXCEPTION);

        // assert requests made by client alpha are served
        assertSanityOfRequestBatch(
                ImmutableList.of(requestForAlpha), ImmutableMap.of(alpha, ImmutableList.of(responseMap.get(alpha))));

        verify(LOCK_CLEANUP_SERVICE_MAP.get(alpha), never()).unlock(any());
        verify(LOCK_CLEANUP_SERVICE_MAP.get(beta)).refreshLockLeases(any());
        verify(LOCK_CLEANUP_SERVICE_MAP.get(beta)).unlock(any());
        verify(NAMESPACE_CACHE_MAP.get(alpha), never()).removeTransactionStateFromCache(anyLong());
        verify(NAMESPACE_CACHE_MAP.get(beta), times(5)).removeTransactionStateFromCache(anyLong());
    }

    @Test
    public void shouldNotFreeResourcesWithinNamespaceIfRequestIsServed() {
        Namespace omega = Namespace.of("omega" + UUID.randomUUID());

        BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>> requestForOmega =
                batchElementForNamespace(omega, PARTITIONED_TIMESTAMPS_LIMIT_PER_SERVER_CALL - 1);
        BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>> secondRequestForOmega =
                batchElementForNamespace(omega, 2);

        UUID requestorId = UUID.randomUUID();

        List<BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>> requests =
                ImmutableList.of(requestForOmega, secondRequestForOmega);

        Map<Namespace, ConjureStartTransactionsResponse> responseMap = startTransactionsResponse(requests, requestorId);

        when(timelockService.startTransactions(any())).thenReturn(responseMap).thenThrow(EXCEPTION);

        assertThatThrownBy(() -> processBatch(timelockService, requestorId, requests))
                .isEqualTo(EXCEPTION);

        // assert the first request made by client omega is served
        assertSanityOfRequestBatch(
                ImmutableList.of(requestForOmega), ImmutableMap.of(omega, ImmutableList.of(responseMap.get(omega))));

        @SuppressWarnings({"unchecked", "rawtypes"})
        ArgumentCaptor<Set<LockToken>> refreshArgumentCaptor =
                (ArgumentCaptor<Set<LockToken>>) ArgumentCaptor.forClass((Class) Set.class);
        verify(LOCK_CLEANUP_SERVICE_MAP.get(omega)).refreshLockLeases(refreshArgumentCaptor.capture());
        verify(LOCK_CLEANUP_SERVICE_MAP.get(omega)).unlock(eq(Collections.emptySet()));
        verify(NAMESPACE_CACHE_MAP.get(omega)).removeTransactionStateFromCache(anyLong());
        Set<LockToken> refreshedTokens = refreshArgumentCaptor.getValue();

        LockToken tokenShare = Futures.getUnchecked(requestForOmega.result())
                .get(0)
                .immutableTimestamp()
                .getLock();
        assertThat(tokenShare).isInstanceOf(LockTokenShare.class).satisfies(token -> {
            LockTokenShare share = ((LockTokenShare) token);
            assertThat(share.sharedLockToken()).isIn(refreshedTokens);
        });
    }

    private BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>
            batchElementForNamespace(Namespace namespace, int numTransactions) {
        return BatchElement.of(
                NamespaceAndRequestParams.of(
                        namespace,
                        RequestParams.of(
                                numTransactions,
                                getCache(namespace),
                                LOCK_CLEANUP_SERVICE_MAP.computeIfAbsent(
                                        namespace, _unused -> mock(LockCleanupService.class)))),
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
            responses.forEach((namespace, response) -> responseMap
                    .computeIfAbsent(namespace, _u -> new ArrayList<>())
                    .add(response));
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

        assertCompletedWithCorrectNumberOfTransactions(requestsForClients);

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

            for (ConjureStartTransactionsResponse conjureResponse : responses) {
                int toIndex =
                        Math.min(startInd + conjureResponse.getTimestamps().count(), startedTransactions.size());

                List<StartIdentifiedAtlasDbTransactionResponse> responseList =
                        startedTransactions.subList(startInd, toIndex);
                startInd = toIndex;

                assertThat(responseList)
                        .satisfies(StartTransactionsTestUtils::assertThatStartTransactionResponsesAreUnique)
                        .allSatisfy(startTxnResponse -> StartTransactionsTestUtils.assertDerivableFromBatchedResponse(
                                startTxnResponse, conjureResponse));
            }
        });
    }

    private void assertCompletedWithCorrectNumberOfTransactions(
            List<BatchElement<NamespaceAndRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>>
                    requestsForClients) {
        requestsForClients.forEach(batchElement -> {
            DisruptorFuture<List<StartIdentifiedAtlasDbTransactionResponse>> resultFuture = batchElement.result();
            NamespaceAndRequestParams requestParams = batchElement.argument();
            assertThat(resultFuture.isDone()).isTrue();

            List<StartIdentifiedAtlasDbTransactionResponse> responseList = Futures.getUnchecked(resultFuture);
            assertThat(responseList).hasSize(requestParams.params().numTransactions());
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

    private LockWatchCache getCache(Namespace namespace) {
        return NAMESPACE_CACHE_MAP.computeIfAbsent(namespace, _u -> spy(LockWatchCacheImpl.noOp()));
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
