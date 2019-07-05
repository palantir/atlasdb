/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.paxos;

import static java.util.stream.Collectors.toList;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.ImmutablePaxosLong;
import com.palantir.paxos.ImmutablePaxosResponses;
import com.palantir.paxos.PaxosLong;
import com.palantir.paxos.PaxosResponses;

@RunWith(MockitoJUnitRunner.class)
public class PaxosQuorumCheckingCoalescingFunctionTests {

    private static final int QUORUM_SIZE = 3;
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    @After
    public void after() {
        executorService.shutdown();
    }

    @Test
    public void successfullyCombinesAndDeconstructs() {
        CoalescingRequestFunction<Long, PaxosLong> node1 = functionFor(
                Maps.immutableEntry(1L, 2L),
                Maps.immutableEntry(2L, 23L),
                Maps.immutableEntry(5L, 65L));

        CoalescingRequestFunction<Long, PaxosLong> node2 = functionFor(
                Maps.immutableEntry(1L, 6L),
                Maps.immutableEntry(2L, 23L),
                Maps.immutableEntry(5L, 32L));

        CoalescingRequestFunction<Long, PaxosLong> node3 = functionFor(
                Maps.immutableEntry(1L, 3L),
                Maps.immutableEntry(2L, 65L),
                Maps.immutableEntry(5L, 32L));

        PaxosQuorumCheckingCoalescingFunction<Long, PaxosLong> paxosQuorumChecker =
                paxosQuorumCheckerFor(node1, node2, node3);

        Map<Long, PaxosResponses<PaxosLong>> expected = ImmutableMap.<Long, PaxosResponses<PaxosLong>>builder()
                .put(1L, sorted(responsesFor(2L, 6L, 3L)))
                .put(2L, sorted(responsesFor(23L, 23L, 65L)))
                .put(5L, sorted(responsesFor(65L, 32L, 32L)))
                .build();

        Map<Long, PaxosResponses<PaxosLong>> results = paxosQuorumChecker.apply(ImmutableSet.of(1L, 2L, 5L));
        assertThat(results.entrySet())
                .allSatisfy(entry -> assertThat(expected).contains(sorted(entry)));
    }

    @Test
    public void individualRequestsCanHaveQuorumFailures() {
        CoalescingRequestFunction<Long, PaxosLong> node1 = functionFor(
                Maps.immutableEntry(1L, 2L),
                Maps.immutableEntry(2L, 23L));

        CoalescingRequestFunction<Long, PaxosLong> node2 = functionFor(
                Maps.immutableEntry(2L, 23L),
                Maps.immutableEntry(5L, 32L));

        CoalescingRequestFunction<Long, PaxosLong> node3 = functionFor(
                Maps.immutableEntry(2L, 65L));

        PaxosQuorumCheckingCoalescingFunction<Long, PaxosLong> paxosQuorumChecker =
                paxosQuorumCheckerFor(node1, node2, node3);

        Map<Long, PaxosResponses<PaxosLong>> expected = ImmutableMap.<Long, PaxosResponses<PaxosLong>>builder()
                .put(1L, sorted(responsesFor(2L)))
                .put(2L, sorted(responsesFor(23L, 23L, 65L)))
                .put(5L, sorted(responsesFor(32L)))
                .build();

        Map<Long, PaxosResponses<PaxosLong>> results = paxosQuorumChecker.apply(ImmutableSet.of(1L, 2L, 5L));
        assertThat(results.entrySet())
                .allSatisfy(entry -> assertThat(expected).contains(sorted(entry)));
    }

    private PaxosQuorumCheckingCoalescingFunction<Long, PaxosLong> paxosQuorumCheckerFor(
            CoalescingRequestFunction<Long, PaxosLong>... nodes) {
        Map<CoalescingRequestFunction<Long, PaxosLong>, ExecutorService> executors =
                Maps.asMap(ImmutableSet.copyOf(nodes), $ -> executorService);
        return new PaxosQuorumCheckingCoalescingFunction<>(ImmutableList.copyOf(nodes), executors, QUORUM_SIZE);
    }

    private static Map.Entry<Long, PaxosResponses<PaxosLong>> sorted(Map.Entry<Long, PaxosResponses<PaxosLong>> entry) {
        return Maps.immutableEntry(entry.getKey(), sorted(entry.getValue()));
    }

    private static PaxosResponses<PaxosLong> sorted(PaxosResponses<PaxosLong> responses) {
        List<PaxosLong> sorted = responses.stream()
                .sorted(Comparator.comparing(PaxosLong::getValue))
                .collect(toList());

        return ImmutablePaxosResponses.of(QUORUM_SIZE, sorted);
    }

    private static CoalescingRequestFunction<Long, PaxosLong> functionFor(Map.Entry<Long, Long>... mappings) {
        Map<Long, Long> function = ImmutableMap.copyOf(ImmutableList.copyOf(mappings));
        return new CoalescingRequestFunction<Long, PaxosLong>() {
            @Override
            public PaxosLong defaultValue() {
                throw new AssertionError("should never get here");
            }

            @Override
            public Map<Long, PaxosLong> apply(Set<Long> request) {
                return KeyedStream.stream(function)
                        .filterKeys(request::contains)
                        .<PaxosLong>map(ImmutablePaxosLong::of)
                        .collectToMap();
            }
        };
    }

    private static PaxosResponses<PaxosLong> responsesFor(Long... longs) {
        return PaxosResponses.of(QUORUM_SIZE, Arrays.stream(longs).map(ImmutablePaxosLong::of).collect(toList()));
    }
}
