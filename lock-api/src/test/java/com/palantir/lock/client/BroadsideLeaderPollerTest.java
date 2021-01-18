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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.timelock.api.LeaderTimes;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.streams.KeyedStream;
import com.palantir.common.time.NanoTime;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LeadershipId;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class BroadsideLeaderPollerTest {
    private static final Namespace NAMESPACE_1 = Namespace.of("tom");
    private static final Namespace NAMESPACE_2 = Namespace.of("jeremy");
    private static final Namespace NAMESPACE_3 = Namespace.of("james");

    private static final LeaderTime LEADER_TIME_1 = LeaderTime.of(LeadershipId.random(), NanoTime.now());
    private static final LeaderTime LEADER_TIME_2 = LeaderTime.of(LeadershipId.random(), NanoTime.now());
    private static final LeaderTime LEADER_TIME_3 = LeaderTime.of(LeadershipId.random(), NanoTime.now());

    private final AuthenticatedInternalMultiClientConjureTimelockService authenticatedService =
            mock(AuthenticatedInternalMultiClientConjureTimelockService.class);
    private final BroadsideLeaderPoller serviceBackedPoller = BroadsideLeaderPoller.create(authenticatedService);

    @Test
    public void routesRequestsCorrectly() {
        when(authenticatedService.leaderTimes(any()))
                .thenReturn(LeaderTimes.builder()
                        .leaderTimes(NAMESPACE_1, LEADER_TIME_1)
                        .leaderTimes(NAMESPACE_2, LEADER_TIME_2)
                        .leaderTimes(NAMESPACE_3, LEADER_TIME_3)
                        .build());

        assertThat(serviceBackedPoller.get(NAMESPACE_1)).isEqualTo(LEADER_TIME_1);
        assertThat(serviceBackedPoller.get(NAMESPACE_2)).isEqualTo(LEADER_TIME_2);
        assertThat(serviceBackedPoller.get(NAMESPACE_3)).isEqualTo(LEADER_TIME_3);

        verify(authenticatedService, times(3)).leaderTimes(any());
    }

    @Test
    public void constructsUnionOfRequestHistory() {
        when(authenticatedService.leaderTimes(any()))
                .thenReturn(LeaderTimes.builder()
                        .leaderTimes(NAMESPACE_1, LEADER_TIME_1)
                        .leaderTimes(NAMESPACE_2, LEADER_TIME_2)
                        .leaderTimes(NAMESPACE_3, LEADER_TIME_3)
                        .build());

        assertThat(serviceBackedPoller.get(NAMESPACE_1)).isEqualTo(LEADER_TIME_1);
        assertThat(serviceBackedPoller.get(NAMESPACE_2)).isEqualTo(LEADER_TIME_2);
        assertThat(serviceBackedPoller.get(NAMESPACE_3)).isEqualTo(LEADER_TIME_3);

        verify(authenticatedService, atLeastOnce()).leaderTimes(ImmutableSet.of(NAMESPACE_1, NAMESPACE_2, NAMESPACE_3));
    }

    @Test
    public void throwsIfResponseRepeatedlyDoesNotContainNamespace() {
        when(authenticatedService.leaderTimes(any()))
                .thenReturn(LeaderTimes.builder()
                        .leaderTimes(NAMESPACE_3, LEADER_TIME_3)
                        .build());

        assertThatThrownBy(() -> serviceBackedPoller.get(NAMESPACE_1))
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("Failed to get leader time for a namespace")
                .hasMessageContaining(NAMESPACE_1.toString());
        verify(authenticatedService, times(5)).leaderTimes(ImmutableSet.of(NAMESPACE_1));
    }

    @Test
    public void recoversIfResponseAddsNamespaceOnLaterAttempt() {
        when(authenticatedService.leaderTimes(any()))
                .thenReturn(LeaderTimes.builder()
                        .leaderTimes(NAMESPACE_3, LEADER_TIME_3)
                        .build())
                .thenReturn(LeaderTimes.builder()
                        .leaderTimes(NAMESPACE_1, LEADER_TIME_1)
                        .build());

        assertThat(serviceBackedPoller.get(NAMESPACE_1)).isEqualTo(LEADER_TIME_1);
    }

    @Test
    public void simulation() {
        when(authenticatedService.leaderTimes(any()))
                .thenReturn(LeaderTimes.builder()
                        .leaderTimes(NAMESPACE_1, LEADER_TIME_1)
                        .build());

        ExecutorService executorService = PTExecutors.newFixedThreadPool(16);
        List<Future<LeaderTime>> leaderTimeFutures = new ArrayList<>();
        for (int request = 0; request < 512; request++) {
            leaderTimeFutures.add(executorService.submit(() -> serviceBackedPoller.get(NAMESPACE_1)));
        }
        leaderTimeFutures.forEach(
                future -> assertThat(Futures.getUnchecked(future)).isEqualTo(LEADER_TIME_1));

        assertThat(mockingDetails(authenticatedService).getInvocations().size())
                .as("some requests were autobatched")
                .isLessThan(512)
                .isGreaterThanOrEqualTo(1);
    }

    @Test
    public void multiClientSimulation() {
        Map<Namespace, LeaderTime> canonicalTimes = KeyedStream.of(
                        IntStream.range(0, 256).boxed().collect(Collectors.toList()))
                .mapKeys(index -> Namespace.of("n" + index))
                .map(unused -> LeaderTime.of(LeadershipId.random(), NanoTime.now()))
                .collectToMap();

        when(authenticatedService.leaderTimes(any())).thenAnswer(invocationOnMock -> {
            Set<Namespace> namespaces = invocationOnMock.getArgument(0);
            return LeaderTimes.of(
                    KeyedStream.of(namespaces.stream()).map(canonicalTimes::get).collectToMap());
        });

        ExecutorService executorService = PTExecutors.newFixedThreadPool(16);
        Multimap<Namespace, Future<LeaderTime>> leaderTimeFutures =
                MultimapBuilder.hashKeys().arrayListValues().build();
        for (int request = 0; request < 1024; request++) {
            Namespace namespace = Namespace.of("n" + (request % 256));
            leaderTimeFutures.put(namespace, executorService.submit(() -> serviceBackedPoller.get(namespace)));
        }

        leaderTimeFutures.forEach((namespace, future) ->
                assertThat(Futures.getUnchecked(future)).isEqualTo(canonicalTimes.get(namespace)));

        assertThat(mockingDetails(authenticatedService).getInvocations().size())
                .as("some requests were autobatched")
                .isLessThan(1024)
                .isGreaterThanOrEqualTo(1);
    }
}
