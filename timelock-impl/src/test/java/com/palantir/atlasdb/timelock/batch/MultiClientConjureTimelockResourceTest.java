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
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.atlasdb.timelock.api.LeaderTimes;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.common.streams.KeyedStream;
import com.palantir.common.time.NanoTime;
import com.palantir.lock.remoting.BlockingTimeoutException;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LeadershipId;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.PartitionedTimestamps;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.tokens.auth.AuthHeader;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

public class MultiClientConjureTimelockResourceTest {
    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("Bearer test");
    private static final int REMOTE_PORT = 4321;
    private static final URL LOCAL = url("https://localhost:1234");
    private static final URL REMOTE = url("https://localhost:" + REMOTE_PORT);
    private static final RedirectRetryTargeter TARGETER =
            RedirectRetryTargeter.create(LOCAL, ImmutableList.of(LOCAL, REMOTE));
    private static final int DUMMY_COMMIT_TS_COUNT = 5;

    private Map<String, AsyncTimelockService> namespaces = new HashMap();
    private Map<String, LeadershipId> namespaceToLeaderMap = new HashMap();
    private Map<String, Integer> namespaceToCommitTsLowerBound = new HashMap();

    private MultiClientConjureTimelockResource resource;

    private PartitionedTimestamps partitionedTimestamps = mock(PartitionedTimestamps.class);
    private LockWatchStateUpdate lockWatchStateUpdate = mock(LockWatchStateUpdate.class);
    private LockImmutableTimestampResponse lockImmutableTimestampResponse = mock(LockImmutableTimestampResponse.class);

    private int commitTsLowerInclusive = 1;

    @Before
    public void before() {
        resource = new MultiClientConjureTimelockResource(TARGETER, this::getServiceForClient);
    }

    @Test
    public void canGetLeaderTimesForMultipleClients() {
        Namespace client1 = Namespace.of("client1");
        Namespace client2 = Namespace.of("client2");
        Set<Namespace> namespaces = ImmutableSet.of(client1, client2);

        LeaderTimes leaderTimesResponse = Futures.getUnchecked(resource.leaderTimes(AUTH_HEADER, namespaces));
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
    public void requestThrowsIfAnyQueryFails() {
        String throwingClient = "alpha";
        Set<Namespace> namespaces = ImmutableSet.of(Namespace.of(throwingClient), Namespace.of("beta"));
        when(getServiceForClient(throwingClient).leaderTime()).thenThrow(new BlockingTimeoutException(""));
        assertThatThrownBy(() -> Futures.getUnchecked(resource.leaderTimes(AUTH_HEADER, namespaces)))
                .isInstanceOf(BlockingTimeoutException.class);
    }

    @Test
    public void canStartTransactionsForMultipleClients() {
        List<String> namespaces = ImmutableList.of("client1", "client2");
        Map<Namespace, ConjureStartTransactionsResponse> startTransactionsResponseMap =
                Futures.getUnchecked(resource.startTransactions(AUTH_HEADER, getStartTransactionsRequests(namespaces)));

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
        assertThat(Futures.getUnchecked(
                        resource.getCommitTimestamps(AUTH_HEADER, getGetCommitTimestampsRequests(namespaces))))
                .isEqualTo(getGetCommitTimestampsResponseMap(namespaces));
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

    private static URL url(String url) {
        try {
            return new URL(url);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}
