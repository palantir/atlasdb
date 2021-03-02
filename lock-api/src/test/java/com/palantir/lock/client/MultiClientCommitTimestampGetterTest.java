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

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher.DisruptorFuture;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.client.MultiClientCommitTimestampGetter.NamespacedRequest;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import one.util.streamex.StreamEx;
import org.junit.Test;

public class MultiClientCommitTimestampGetterTest {
    private static final Map<Namespace, Long> LOWEST_START_TS_MAP = new HashMap<>();
    private static final int COMMIT_TS_LIMIT_PER_REQUEST = 5;
    private static final SafeIllegalStateException EXCEPTION = new SafeIllegalStateException("Something went wrong!");
    private static final Map<Namespace, LockWatchEventCache> LOCK_WATCH_EVENT_CACHE_MAP_MAP = new HashMap<>();

    private final LockToken lockToken = mock(LockToken.class);
    private final InternalMultiClientConjureTimelockService timelockService =
            mock(InternalMultiClientConjureTimelockService.class);
    private final LockWatchStateUpdate lockWatchStateUpdate = mock(LockWatchStateUpdate.class);

    private final Consumer<List<BatchElement<NamespacedRequest, Long>>> consumer =
            MultiClientCommitTimestampGetter.consumer(timelockService);

    @Test
    public void canServiceOneClient() {
        setupServiceAndAssertSanityOfResponse(getCommitTimestampRequestsForClients(1, COMMIT_TS_LIMIT_PER_REQUEST - 1));
    }

    @Test
    public void canServiceOneClientInMultipleRequests() {
        setupServiceAndAssertSanityOfResponse(getCommitTimestampRequestsForClients(1, COMMIT_TS_LIMIT_PER_REQUEST * 5));
    }

    @Test
    public void canServiceMultipleClients() {
        int clientCount = 50;
        setupServiceAndAssertSanityOfResponse(
                getCommitTimestampRequestsForClients(clientCount, (COMMIT_TS_LIMIT_PER_REQUEST - 1) * clientCount));
    }

    @Test
    public void canServiceMultipleClientsWithMultipleServerCalls() {
        int clientCount = 5;
        setupServiceAndAssertSanityOfResponse(
                getCommitTimestampRequestsForClients(clientCount, (COMMIT_TS_LIMIT_PER_REQUEST + 1) * clientCount));
    }

    @Test
    public void updatesCacheWhileProcessingResponse() {
        Namespace client = Namespace.of("Kitty");
        List<BatchElement<NamespacedRequest, Long>> batchElements = IntStream.range(0, COMMIT_TS_LIMIT_PER_REQUEST * 2)
                .mapToObj(ind -> batchElementForNamespace(client))
                .collect(toList());
        setupServiceAndAssertSanityOfResponse(batchElements);

        LockWatchEventCache cache = LOCK_WATCH_EVENT_CACHE_MAP_MAP.get(client);
        verify(cache, times(2)).lastKnownVersion();
        verify(cache, times(2)).processGetCommitTimestampsUpdate(any(), any());
    }

    @Test
    public void doesNotUpdateCacheIfClientNotServed() {
        Namespace alpha = Namespace.of("alpha" + UUID.randomUUID());
        Namespace beta = Namespace.of("beta" + UUID.randomUUID());

        BatchElement<NamespacedRequest, Long> requestForAlpha = batchElementForNamespace(alpha);
        BatchElement<NamespacedRequest, Long> requestForBeta = batchElementForNamespace(beta);

        List<BatchElement<NamespacedRequest, Long>> allRequests = ImmutableList.of(requestForAlpha, requestForBeta);
        List<BatchElement<NamespacedRequest, Long>> alphaRequestList = ImmutableList.of(requestForAlpha);
        Map<Namespace, GetCommitTimestampsResponse> responseMap = getCommitTimestamps(alphaRequestList);

        when(timelockService.getCommitTimestamps(any())).thenReturn(responseMap).thenThrow(EXCEPTION);

        assertThatThrownBy(() -> consumer.accept(allRequests)).isEqualTo(EXCEPTION);

        // assert requests made by client alpha are served
        assertSanityOfResponse(alphaRequestList, ImmutableMap.of(alpha, ImmutableList.of(responseMap.get(alpha))));

        LockWatchEventCache alphaCache = LOCK_WATCH_EVENT_CACHE_MAP_MAP.get(alpha);
        verify(alphaCache).lastKnownVersion();
        verify(alphaCache).processGetCommitTimestampsUpdate(any(), any());

        // assert unsuccessful requests were made by client beta
        assertThat(requestForBeta.result().isDone()).isFalse();

        LockWatchEventCache betaCache = LOCK_WATCH_EVENT_CACHE_MAP_MAP.get(beta);
        verify(betaCache, times(2)).lastKnownVersion();
        verify(betaCache, never()).processGetCommitTimestampsUpdate(any(), any());
    }

    private void setupServiceAndAssertSanityOfResponse(List<BatchElement<NamespacedRequest, Long>> batch) {
        Map<Namespace, List<GetCommitTimestampsResponse>> expectedResponseMap = new HashMap<>();

        when(timelockService.getCommitTimestamps(any())).thenAnswer(invocation -> {
            Map<Namespace, GetCommitTimestampsResponse> commitTimestamps =
                    getCommitTimestampResponse(invocation.getArgument(0));
            commitTimestamps.forEach((namespace, response) -> {
                expectedResponseMap
                        .computeIfAbsent(namespace, _unused -> new ArrayList())
                        .add(response);
            });
            return commitTimestamps;
        });

        consumer.accept(batch);
        assertSanityOfResponse(batch, expectedResponseMap);
    }

    private void assertSanityOfResponse(
            List<BatchElement<NamespacedRequest, Long>> batch,
            Map<Namespace, List<GetCommitTimestampsResponse>> expectedResponseMap) {
        // all requests should be served
        assertThat(batch.stream().filter(elem -> !elem.result().isDone()).collect(Collectors.toSet()))
                .isEmpty();

        Map<Namespace, List<Long>> partitionedResponseMap = batch.stream()
                .collect(groupingBy(
                        elem -> elem.argument().namespace(),
                        Collectors.mapping(elem -> Futures.getUnchecked(elem.result()), toList())));

        assertThat(partitionedResponseMap.keySet()).isEqualTo(expectedResponseMap.keySet());
        assertCorrectnessOfCompletedRequests(expectedResponseMap, partitionedResponseMap);
    }

    private static void assertCorrectnessOfCompletedRequests(
            Map<Namespace, List<GetCommitTimestampsResponse>> expectedResponseMap,
            Map<Namespace, List<Long>> partitionedResponseMap) {
        KeyedStream.stream(partitionedResponseMap)
                .forEach(((namespace, commitTsList) ->
                        assertSanityOfServedTimestamps(commitTsList, expectedResponseMap.get(namespace))));
    }

    private static void assertSanityOfServedTimestamps(
            List<Long> commitTsList, List<GetCommitTimestampsResponse> commitTimestampsResponses) {
        long requestedCommitTsCount = commitTimestampsResponses.stream()
                .mapToLong(resp -> resp.getInclusiveUpper() - resp.getInclusiveLower() + 1)
                .sum();
        assertThat(requestedCommitTsCount).isEqualTo(commitTsList.size());
        assertThat(ImmutableSet.copyOf(commitTsList)).hasSameSizeAs(commitTsList);
        assertThat(StreamEx.of(commitTsList)
                        .pairMap((first, second) -> first > second)
                        .anyMatch(x -> x))
                .isFalse();
    }

    private Map<Namespace, GetCommitTimestampsResponse> getCommitTimestamps(
            List<BatchElement<NamespacedRequest, Long>> batch) {
        Map<Namespace, List<BatchElement<NamespacedRequest, Long>>> partitionedRequests =
                batch.stream().collect(groupingBy(elem -> elem.argument().namespace(), toList()));
        return getCommitTimestampResponse(KeyedStream.stream(partitionedRequests)
                .map(requestList -> GetCommitTimestampsRequest.builder()
                        .numTimestamps(requestList.size())
                        .build())
                .collectToMap());
    }

    private Map<Namespace, GetCommitTimestampsResponse> getCommitTimestampResponse(
            Map<Namespace, GetCommitTimestampsRequest> requestMap) {
        return KeyedStream.stream(requestMap)
                .mapEntries((namespace, request) -> {
                    long inclusiveLower = getLowerBound(namespace);
                    long exclusiveUpper =
                            inclusiveLower + Math.min(request.getNumTimestamps(), COMMIT_TS_LIMIT_PER_REQUEST);
                    updateLowerBound(namespace, exclusiveUpper);
                    return Maps.immutableEntry(
                            namespace,
                            GetCommitTimestampsResponse.builder()
                                    .inclusiveLower(inclusiveLower)
                                    .inclusiveUpper(exclusiveUpper - 1)
                                    .lockWatchUpdate(lockWatchStateUpdate)
                                    .build());
                })
                .collectToMap();
    }

    private long getLowerBound(Namespace namespace) {
        return LOWEST_START_TS_MAP.getOrDefault(namespace, 1L);
    }

    private void updateLowerBound(Namespace namespace, long numTimestamps) {
        LOWEST_START_TS_MAP.put(namespace, LOWEST_START_TS_MAP.getOrDefault(namespace, 1L) + numTimestamps);
    }

    private List<BatchElement<NamespacedRequest, Long>> getCommitTimestampRequestsForClients(
            int clientCount, int requestCount) {
        List<BatchElement<NamespacedRequest, Long>> test = IntStream.range(0, requestCount)
                .mapToObj(ind -> batchElementForNamespace(Namespace.of("Test_" + (ind % clientCount))))
                .collect(Collectors.toList());
        return test;
    }

    private BatchElement<NamespacedRequest, Long> batchElementForNamespace(Namespace namespace) {
        return BatchElement.of(
                ImmutableNamespacedRequest.builder()
                        .namespace(namespace)
                        .startTs(1)
                        .cache(LOCK_WATCH_EVENT_CACHE_MAP_MAP.computeIfAbsent(
                                namespace, _unused -> mock(LockWatchEventCache.class)))
                        .commitLocksToken(lockToken)
                        .build(),
                new DisruptorFuture<Long>("test"));
    }
}
