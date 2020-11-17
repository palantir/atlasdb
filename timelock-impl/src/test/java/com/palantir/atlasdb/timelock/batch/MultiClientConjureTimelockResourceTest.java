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
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.atlasdb.timelock.api.NamespacedGetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.NamespacedGetCommitTimestampsResponse;
import com.palantir.atlasdb.timelock.api.NamespacedLeaderTime;
import com.palantir.atlasdb.timelock.api.NamespacedStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.NamespacedStartTransactionsResponse;
import com.palantir.lock.remoting.BlockingTimeoutException;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.PartitionedTimestamps;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.tokens.auth.AuthHeader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
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
    private static final int COMMIT_TS_LOWER_INCLUSIVE = 1;
    private static final int COMMIT_TS_UPPER_INCLUSIVE = 5;

    private AsyncTimelockService timelockService = mock(AsyncTimelockService.class);
    private LeaderTime leaderTime = mock(LeaderTime.class);
    private LockWatchStateUpdate lockWatchStateUpdate = mock(LockWatchStateUpdate.class);
    private PartitionedTimestamps partitionedTimestamps = mock(PartitionedTimestamps.class);
    private Lease lease = mock(Lease.class);
    private LockImmutableTimestampResponse lockImmutableTimestampResponse = mock(LockImmutableTimestampResponse.class);
    private MultiClientConjureTimelockResource resource;

    @Before
    public void before() {
        resource = new MultiClientConjureTimelockResource(TARGETER, unused -> timelockService);
    }

    @Test
    public void canGetLeaderTimesForMultipleClients() {
        when(timelockService.leaderTime()).thenReturn(Futures.immediateFuture(leaderTime));
        Set<String> namespaces = ImmutableSet.of("client1", "client2");
        assertThat(Futures.getUnchecked(resource.leaderTimes(AUTH_HEADER, namespaces)))
                .isEqualTo(getLeaderTimesForNamespaces(namespaces));
    }

    @Test
    public void canGetCommitTimestampsForMultipleClients() {
        GetCommitTimestampsResponse getCommitTimestampsResponse = GetCommitTimestampsResponse.of(
                COMMIT_TS_LOWER_INCLUSIVE, COMMIT_TS_UPPER_INCLUSIVE, lockWatchStateUpdate);

        when(timelockService.getCommitTimestamps(anyInt(), any()))
                .thenReturn(Futures.immediateFuture(getCommitTimestampsResponse));

        Set<String> namespaces = ImmutableSet.of("client1", "client2");
        assertThat(Futures.getUnchecked(
                        resource.getCommitTimestamps(AUTH_HEADER, getGetCommitTimestampsRequests(namespaces))))
                .isEqualTo(getGetCommitTimestampsResponseList(namespaces));
    }

    @Test
    public void canStartTransactionsForMultipleClients() {
        configureStartTransactionsEndPoint();
        List<String> namespaces = ImmutableList.of("client1", "client2");
        assertThat(Futures.getUnchecked(
                        resource.startTransactions(AUTH_HEADER, getStartTransactionsRequests(namespaces))))
                .isEqualTo(getStartTransactionsResponseList(namespaces));
    }

    @Test
    public void startTransactionsThrowsWhenMultipleRequestorsQueryForSameNamespace() {
        configureStartTransactionsEndPoint();
        assertThatThrownBy(() -> resource.startTransactions(
                        AUTH_HEADER, getStartTransactionsRequests(ImmutableList.of("client1", "client1"))))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("More than one TimeLock client is requesting to start transactions");
    }

    @Test
    public void requestThrowsIfAnyQueryFails() {
        when(timelockService.leaderTime())
                .thenReturn(Futures.immediateFuture(leaderTime))
                .thenThrow(new BlockingTimeoutException(""));
        Set<String> namespaces = ImmutableSet.of("client1", "client2");
        assertThatThrownBy(() -> Futures.getUnchecked(resource.leaderTimes(AUTH_HEADER, namespaces)))
                .isInstanceOf(BlockingTimeoutException.class);
    }

    private List<NamespacedGetCommitTimestampsResponse> getGetCommitTimestampsResponseList(Set<String> namespaces) {
        return namespaces.stream()
                .map(namespace -> NamespacedGetCommitTimestampsResponse.builder()
                        .namespace(namespace)
                        .inclusiveLower(COMMIT_TS_LOWER_INCLUSIVE)
                        .inclusiveUpper(COMMIT_TS_UPPER_INCLUSIVE)
                        .lockWatchUpdate(lockWatchStateUpdate)
                        .build())
                .collect(Collectors.toList());
    }

    private List<NamespacedStartTransactionsResponse> getStartTransactionsResponseList(List<String> namespaces) {
        return namespaces.stream()
                .map(namespace -> NamespacedStartTransactionsResponse.builder()
                        .namespace(namespace)
                        .immutableTimestamp(lockImmutableTimestampResponse)
                        .lease(lease)
                        .timestamps(partitionedTimestamps)
                        .lockWatchUpdate(lockWatchStateUpdate)
                        .build())
                .collect(Collectors.toList());
    }

    private List<NamespacedGetCommitTimestampsRequest> getGetCommitTimestampsRequests(Set<String> namespaces) {
        return namespaces.stream()
                .map(namespace -> NamespacedGetCommitTimestampsRequest.builder()
                        .namespace(namespace)
                        .numTimestamps(4)
                        .build())
                .collect(Collectors.toList());
    }

    private List<NamespacedStartTransactionsRequest> getStartTransactionsRequests(List<String> namespaces) {
        return namespaces.stream()
                .map(namespace -> NamespacedStartTransactionsRequest.builder()
                        .namespace(namespace)
                        .numTransactions(5)
                        .requestId(UUID.randomUUID())
                        .requestorId(UUID.randomUUID())
                        .build())
                .collect(Collectors.toList());
    }

    private List<NamespacedLeaderTime> getLeaderTimesForNamespaces(Set<String> namespaces) {
        return namespaces.stream()
                .map(namespace -> NamespacedLeaderTime.of(namespace, leaderTime))
                .collect(Collectors.toList());
    }

    public void configureStartTransactionsEndPoint() {
        ConjureStartTransactionsResponse startTransactionsResponse = ConjureStartTransactionsResponse.builder()
                .immutableTimestamp(lockImmutableTimestampResponse)
                .lease(lease)
                .timestamps(partitionedTimestamps)
                .lockWatchUpdate(lockWatchStateUpdate)
                .build();

        when(timelockService.startTransactionsWithWatches(any()))
                .thenReturn(Futures.immediateFuture(startTransactionsResponse));
    }

    private static URL url(String url) {
        try {
            return new URL(url);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}
