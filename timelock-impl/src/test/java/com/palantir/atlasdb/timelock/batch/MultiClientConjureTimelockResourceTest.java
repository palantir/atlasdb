/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.batch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.common.api.timelock.TimestampLeaseName;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.api.ConjureLockTokenV2;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.ConjureTimestampRange;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponseV2;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.atlasdb.timelock.api.GetMinLeasedTimestampRequests;
import com.palantir.atlasdb.timelock.api.GetMinLeasedTimestampResponses;
import com.palantir.atlasdb.timelock.api.LeaderTimes;
import com.palantir.atlasdb.timelock.api.LeaseGuarantee;
import com.palantir.atlasdb.timelock.api.LeaseIdentifier;
import com.palantir.atlasdb.timelock.api.MultiClientGetMinLeasedTimestampRequest;
import com.palantir.atlasdb.timelock.api.MultiClientGetMinLeasedTimestampResponse;
import com.palantir.atlasdb.timelock.api.MultiClientTimestampLeaseRequest;
import com.palantir.atlasdb.timelock.api.MultiClientTimestampLeaseResponse;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.NamespaceTimestampLeaseRequest;
import com.palantir.atlasdb.timelock.api.NamespaceTimestampLeaseResponse;
import com.palantir.atlasdb.timelock.api.RequestId;
import com.palantir.atlasdb.timelock.api.TimestampLeaseRequests;
import com.palantir.atlasdb.timelock.api.TimestampLeaseResponse;
import com.palantir.atlasdb.timelock.api.TimestampLeaseResponses;
import com.palantir.atlasdb.util.TimelockTestUtils;
import com.palantir.common.streams.KeyedStream;
import com.palantir.common.time.NanoTime;
import com.palantir.conjure.java.api.errors.QosException.RetryOther;
import com.palantir.conjure.java.api.errors.QosException.Throttle;
import com.palantir.conjure.java.undertow.lib.RequestContext;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.lock.remoting.BlockingTimeoutException;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LeadershipId;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.PartitionedTimestamps;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.tokens.auth.AuthHeader;
import java.net.URL;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MultiClientConjureTimelockResourceTest {
    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("Bearer test");
    private static final int REMOTE_PORT = 4321;
    private static final URL LOCAL = TimelockTestUtils.url("https://localhost:1234");
    private static final URL REMOTE = TimelockTestUtils.url("https://localhost:" + REMOTE_PORT);
    private static final RedirectRetryTargeter TARGETER =
            RedirectRetryTargeter.create(LOCAL, ImmutableList.of(LOCAL, REMOTE));
    private static final int DUMMY_COMMIT_TS_COUNT = 5;

    private Map<String, AsyncTimelockService> namespaces = new HashMap<>();
    private Map<String, LeadershipId> namespaceToLeaderMap = new HashMap<>();
    private Map<String, Integer> namespaceToCommitTsLowerBound = new HashMap<>();

    private MultiClientConjureTimelockResource resource;

    private PartitionedTimestamps partitionedTimestamps = mock(PartitionedTimestamps.class);
    private LockWatchStateUpdate lockWatchStateUpdate =
            LockWatchStateUpdate.success(UUID.randomUUID(), 5L, ImmutableList.of());
    private LockImmutableTimestampResponse lockImmutableTimestampResponse = mock(LockImmutableTimestampResponse.class);
    private static final RequestContext REQUEST_CONTEXT = null;

    private int commitTsLowerInclusive = 1;

    @BeforeEach
    public void before() {
        resource = new MultiClientConjureTimelockResource(
                TARGETER, (namespace, _context) -> getServiceForClient(namespace));
    }

    @Test
    public void canGetLeaderTimesForMultipleClients() {
        Namespace client1 = Namespace.of("client1");
        Namespace client2 = Namespace.of("client2");
        Set<Namespace> namespaces = ImmutableSet.of(client1, client2);

        LeaderTimes leaderTimesResponse =
                Futures.getUnchecked(resource.leaderTimes(AUTH_HEADER, namespaces, REQUEST_CONTEXT));
        Map<Namespace, LeaderTime> leaderTimes = leaderTimesResponse.getLeaderTimes();

        // leaderTimes for namespaces are computed by their respective underlying AsyncTimelockService instances
        leaderTimes.forEach((namespace, leaderTime) -> {
            assertThat(leaderTime.id()).isEqualTo(namespaceToLeaderMap.get(namespace.get()));
        });

        // there should be as many leaders as there are distinct clients
        Set<UUID> leaders = leaderTimes.values().stream()
                .map(LeaderTime::id)
                .map(LeadershipId::id)
                .collect(Collectors.toSet());
        assertThat(leaders).hasSameSizeAs(namespaces);
    }

    @Test
    public void requestHandlesExceptionAndThrowsIfAnyQueryFails() {
        String throwingClient = "alpha";
        Set<Namespace> namespaces = ImmutableSet.of(Namespace.of(throwingClient), Namespace.of("beta"));
        when(getServiceForClient(throwingClient).leaderTime()).thenThrow(new BlockingTimeoutException(""));
        assertThatThrownBy(() -> Futures.getUnchecked(resource.leaderTimes(AUTH_HEADER, namespaces, REQUEST_CONTEXT)))
                .hasCauseInstanceOf(Throttle.class);
    }

    @Test
    public void handlesNotCurrentLeaderExceptions() {
        String throwingClient = "alpha";
        Set<Namespace> namespaces = ImmutableSet.of(Namespace.of(throwingClient), Namespace.of("beta"));
        when(getServiceForClient(throwingClient).leaderTime())
                .thenThrow(new NotCurrentLeaderException("Not the leader!"));
        assertThatThrownBy(() -> Futures.getUnchecked(resource.leaderTimes(AUTH_HEADER, namespaces, REQUEST_CONTEXT)))
                .hasCauseInstanceOf(RetryOther.class)
                .hasRootCauseMessage("Suggesting request retry against: " + REMOTE);
    }

    @Test
    public void canStartTransactionsForMultipleClients() {
        List<String> namespaces = ImmutableList.of("client1", "client2");
        Map<Namespace, ConjureStartTransactionsResponse> startTransactionsResponseMap =
                Futures.getUnchecked(resource.startTransactionsForClients(
                        AUTH_HEADER, getStartTransactionsRequests(namespaces), REQUEST_CONTEXT));

        startTransactionsResponseMap.forEach((namespace, response) -> {
            assertThat(response.getLease().leaderTime().id()).isEqualTo(namespaceToLeaderMap.get(namespace.get()));
        });

        Set<LeadershipId> leadershipIds = startTransactionsResponseMap.values().stream()
                .map(ConjureStartTransactionsResponse::getLease)
                .map(Lease::leaderTime)
                .map(LeaderTime::id)
                .collect(Collectors.toSet());
        assertThat(leadershipIds).hasSameSizeAs(namespaces);
    }

    @Test
    public void canGetCommitTimestampsForMultipleClients() {
        Set<String> namespaces = ImmutableSet.of("client1", "client2");
        assertThat(Futures.getUnchecked(resource.getCommitTimestampsForClients(
                        AUTH_HEADER, getGetCommitTimestampsRequests(namespaces), REQUEST_CONTEXT)))
                .containsExactlyInAnyOrderEntriesOf(getGetCommitTimestampsResponseMap(namespaces));
    }

    @Test
    public void canUnlockForMultipleClients() {
        Set<String> namespaces = ImmutableSet.of("client1", "client2");
        Map<Namespace, ConjureUnlockRequestV2> requests = getUnlockRequests(namespaces);
        Map<Namespace, ConjureUnlockResponseV2> responses =
                Futures.getUnchecked(resource.unlock(AUTH_HEADER, requests, REQUEST_CONTEXT));
        for (Map.Entry<Namespace, ConjureUnlockRequestV2> request : requests.entrySet()) {
            assertThat(responses.get(request.getKey()).get())
                    .containsExactlyElementsOf(request.getValue().get());
        }
    }

    @Test
    public void canAcquireNamedMinTimestampLease() {
        Namespace client1 = Namespace.of("client1");
        Namespace client2 = Namespace.of("client2");

        TimestampLeaseName timestampName1 = TimestampLeaseName.of("t1");
        TimestampLeaseName timestampName2 = TimestampLeaseName.of("t2");
        TimestampLeaseName timestampName3 = TimestampLeaseName.of("t");

        TimestampLeaseRequests request1ForClient1 = createTimestampLeasesRequest(timestampName1, timestampName2);
        TimestampLeaseRequests request2ForClient1 = createTimestampLeasesRequest(timestampName1, timestampName3);
        TimestampLeaseRequests requestForClient2 =
                createTimestampLeasesRequest(timestampName1, timestampName2, timestampName3);

        TimestampLeaseResponses response1ForClient1 = createTimestampLeasesResponseFor(request1ForClient1);
        TimestampLeaseResponses response2ForClient1 = createTimestampLeasesResponseFor(request2ForClient1);
        TimestampLeaseResponses responseForClient2 = createTimestampLeasesResponseFor(requestForClient2);

        AsyncTimelockService serviceForClient1 = getServiceForClient(client1.get());
        stubAcquireNamedMinTimestampLeaseInResource(serviceForClient1, request1ForClient1, response1ForClient1);
        stubAcquireNamedMinTimestampLeaseInResource(serviceForClient1, request2ForClient1, response2ForClient1);

        AsyncTimelockService serviceForClient2 = getServiceForClient(client2.get());
        stubAcquireNamedMinTimestampLeaseInResource(serviceForClient2, requestForClient2, responseForClient2);

        MultiClientTimestampLeaseRequest request = MultiClientTimestampLeaseRequest.of(Map.of(
                client1, NamespaceTimestampLeaseRequest.of(List.of(request1ForClient1, request2ForClient1)),
                client2, NamespaceTimestampLeaseRequest.of(List.of(requestForClient2))));
        MultiClientTimestampLeaseResponse response = MultiClientTimestampLeaseResponse.of(Map.of(
                client1, NamespaceTimestampLeaseResponse.of(List.of(response1ForClient1, response2ForClient1)),
                client2, NamespaceTimestampLeaseResponse.of(List.of(responseForClient2))));
        assertThat(Futures.getUnchecked(resource.acquireTimestampLease(AUTH_HEADER, request, REQUEST_CONTEXT)))
                .isEqualTo(response);
    }

    @Test
    public void canGetNamedMinTimestamp() {
        Namespace client1 = Namespace.of("client1");
        Namespace client2 = Namespace.of("client2");

        TimestampLeaseName timestampName1 = TimestampLeaseName.of("t1");
        TimestampLeaseName timestampName2 = TimestampLeaseName.of("t2");

        long timestamp1 = 1L;
        long timestamp2 = 2L;
        long timestamp3 = 3L;

        AsyncTimelockService serviceForClient1 = getServiceForClient(client1.get());
        stubGetMinLeasedNamedTimestampInResource(serviceForClient1, timestampName1, timestamp1);
        stubGetMinLeasedNamedTimestampInResource(serviceForClient1, timestampName2, timestamp2);

        AsyncTimelockService serviceForClient2 = getServiceForClient(client2.get());
        stubGetMinLeasedNamedTimestampInResource(serviceForClient2, timestampName2, timestamp3);

        MultiClientGetMinLeasedTimestampRequest request = MultiClientGetMinLeasedTimestampRequest.of(Map.of(
                client1, GetMinLeasedTimestampRequests.of(List.of(timestampName1, timestampName2)),
                client2, GetMinLeasedTimestampRequests.of(List.of(timestampName2))));
        MultiClientGetMinLeasedTimestampResponse response = MultiClientGetMinLeasedTimestampResponse.of(Map.of(
                client1,
                GetMinLeasedTimestampResponses.of(Map.of(
                        timestampName1, timestamp1,
                        timestampName2, timestamp2)),
                client2,
                GetMinLeasedTimestampResponses.of(Map.of(timestampName2, timestamp3))));
        assertThat(Futures.getUnchecked(resource.getMinLeasedTimestamp(AUTH_HEADER, request, REQUEST_CONTEXT)))
                .isEqualTo(response);
    }

    private Map<Namespace, GetCommitTimestampsResponse> getGetCommitTimestampsResponseMap(Set<String> namespaces) {
        return KeyedStream.of(namespaces)
                .mapKeys(Namespace::of)
                .map(this::getCommitTimestampResponse)
                .collectToMap();
    }

    private Map<Namespace, GetCommitTimestampsRequest> getGetCommitTimestampsRequests(Set<String> namespaces) {
        return KeyedStream.of(namespaces)
                .mapKeys(Namespace::of)
                .map(namespace -> GetCommitTimestampsRequest.builder()
                        .numTimestamps(DUMMY_COMMIT_TS_COUNT)
                        .build())
                .collectToMap();
    }

    private Map<Namespace, ConjureStartTransactionsRequest> getStartTransactionsRequests(List<String> namespaces) {
        return KeyedStream.of(namespaces)
                .map(namespace -> ConjureStartTransactionsRequest.builder()
                        .numTransactions(5)
                        .requestId(UUID.randomUUID())
                        .requestorId(UUID.randomUUID())
                        .build())
                .mapKeys(Namespace::of)
                .collectToMap();
    }

    private Map<Namespace, ConjureUnlockRequestV2> getUnlockRequests(Set<String> namespaces) {
        return KeyedStream.of(namespaces)
                .map(namespace -> ConjureUnlockRequestV2.of(ImmutableSet.of(ConjureLockTokenV2.of(UUID.randomUUID()))))
                .mapKeys(Namespace::of)
                .collectToMap();
    }

    private AsyncTimelockService getServiceForClient(String client) {
        return namespaces.computeIfAbsent(client, this::createAsyncTimeLockServiceForClient);
    }

    private AsyncTimelockService createAsyncTimeLockServiceForClient(String client) {
        AsyncTimelockService timelockService = mock(AsyncTimelockService.class);
        LeadershipId leadershipId = namespaceToLeaderMap.computeIfAbsent(client, _u -> LeadershipId.random());
        LeaderTime leaderTime = LeaderTime.of(leadershipId, NanoTime.createForTests(1L));
        when(timelockService.leaderTime()).thenReturn(Futures.immediateFuture(leaderTime));
        when(timelockService.startTransactionsWithWatches(any()))
                .thenReturn(Futures.immediateFuture(ConjureStartTransactionsResponse.builder()
                        .immutableTimestamp(lockImmutableTimestampResponse)
                        .lease(Lease.of(leaderTime, Duration.ofSeconds(977)))
                        .timestamps(partitionedTimestamps)
                        .lockWatchUpdate(lockWatchStateUpdate)
                        .build()));
        when(timelockService.getCommitTimestamps(anyInt(), any()))
                .thenReturn(Futures.immediateFuture(getCommitTimestampResponse(client)));
        when(timelockService.unlock(any()))
                .thenAnswer(invocation -> Futures.immediateFuture(invocation.<Set<LockToken>>getArgument(0)));
        return timelockService;
    }

    private GetCommitTimestampsResponse getCommitTimestampResponse(String namespace) {
        int inclusiveLower = getInclusiveLowerCommitTs(namespace);
        return GetCommitTimestampsResponse.of(
                inclusiveLower, inclusiveLower + DUMMY_COMMIT_TS_COUNT, lockWatchStateUpdate);
    }

    private Integer getInclusiveLowerCommitTs(String namespace) {
        return namespaceToCommitTsLowerBound.computeIfAbsent(namespace, _u -> commitTsLowerInclusive++);
    }

    private static void stubAcquireNamedMinTimestampLeaseInResource(
            AsyncTimelockService service, TimestampLeaseRequests request, TimestampLeaseResponses response) {
        when(service.acquireTimestampLease(request.getRequestId().get(), request.getNumFreshTimestamps()))
                .thenReturn(Futures.immediateFuture(response));
    }

    private static void stubGetMinLeasedNamedTimestampInResource(
            AsyncTimelockService service, TimestampLeaseName timestampName, long timestamp) {
        when(service.getMinLeasedTimestamp(timestampName)).thenReturn(Futures.immediateFuture(timestamp));
    }

    private static TimestampLeaseRequests createTimestampLeasesRequest(TimestampLeaseName... names) {
        TimestampLeaseRequests.Builder builder =
                TimestampLeaseRequests.builder().requestId(RequestId.of(UUID.randomUUID()));

        Map<TimestampLeaseName, Integer> numFreshTimestamps = KeyedStream.of(Arrays.stream(names))
                .map(_name -> createRandomPositiveInteger())
                .collectToMap();

        return builder.numFreshTimestamps(numFreshTimestamps).build();
    }

    private static TimestampLeaseResponses createTimestampLeasesResponseFor(TimestampLeaseRequests request1ForClient1) {
        return TimestampLeaseResponses.builder()
                .leaseGuarantee(createRandomLeaseGuarantee(request1ForClient1.getRequestId()))
                .timestampLeaseResponses(KeyedStream.stream(request1ForClient1.getNumFreshTimestamps())
                        .map(_name -> createTimestampLeaseResponse())
                        .collectToMap())
                .build();
    }

    private static TimestampLeaseResponse createTimestampLeaseResponse() {
        return TimestampLeaseResponse.builder()
                .minLeased(createRandomPositiveInteger())
                .freshTimestamps(ConjureTimestampRange.of(createRandomPositiveInteger(), createRandomPositiveInteger()))
                .build();
    }

    private static LeaseGuarantee createRandomLeaseGuarantee(RequestId requestId) {
        return LeaseGuarantee.builder()
                .lease(createRandomLease())
                .identifier(LeaseIdentifier.of(requestId.get()))
                .build();
    }

    private static Lease createRandomLease() {
        return Lease.of(LeaderTime.of(LeadershipId.random(), NanoTime.createForTests(1234L)), Duration.ofDays(2000));
    }

    private static int createRandomPositiveInteger() {
        return ThreadLocalRandom.current().nextInt(0, 2500);
    }
}
