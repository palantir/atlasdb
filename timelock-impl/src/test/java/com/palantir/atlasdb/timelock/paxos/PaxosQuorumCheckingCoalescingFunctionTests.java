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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.ImmutablePaxosLong;
import com.palantir.paxos.PaxosLong;
import com.palantir.paxos.PaxosResponsesWithRemote;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

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
        TestFunction node1 = functionFor(
                Maps.immutableEntry(1L, 2L),
                Maps.immutableEntry(2L, 23L),
                Maps.immutableEntry(5L, 65L));

        TestFunction node2 = functionFor(
                Maps.immutableEntry(1L, 6L),
                Maps.immutableEntry(2L, 23L),
                Maps.immutableEntry(5L, 32L));

        TestFunction node3 = functionFor(
                Maps.immutableEntry(1L, 3L),
                Maps.immutableEntry(2L, 65L),
                Maps.immutableEntry(5L, 32L));

        PaxosQuorumCheckingCoalescingFunction<Long, PaxosLong, TestFunction> paxosQuorumChecker =
                paxosQuorumCheckerFor(node1, node2, node3);

        Map<Long, PaxosResponsesWithRemote<TestFunction, PaxosLong>> expected = ImmutableMap.<Long, PaxosResponsesWithRemote<TestFunction, PaxosLong>>builder()
                .put(1L, responsesFor(entry(node1, 2L), entry(node2, 6L), entry(node3, 3L)))
                .put(2L, responsesFor(entry(node1,23L), entry(node2, 23L), entry(node3, 65L)))
                .put(5L, responsesFor(entry(node1, 65L), entry(node2, 32L), entry(node3, 32L)))
                .build();

        Map<Long, PaxosResponsesWithRemote<TestFunction, PaxosLong>> results =
                paxosQuorumChecker.apply(ImmutableSet.of(1L, 2L, 5L));

        assertThat(results).isEqualTo(expected);
    }

    @Test
    public void individualRequestsCanHaveQuorumFailures() {
        TestFunction node1 = functionFor(
                Maps.immutableEntry(1L, 2L),
                Maps.immutableEntry(2L, 23L));

        TestFunction node2 = functionFor(
                Maps.immutableEntry(2L, 23L),
                Maps.immutableEntry(5L, 32L));

        TestFunction node3 = functionFor(
                Maps.immutableEntry(2L, 65L));

        PaxosQuorumCheckingCoalescingFunction<Long, PaxosLong, TestFunction> paxosQuorumChecker =
                paxosQuorumCheckerFor(node1, node2, node3);

        Map<Long, PaxosResponsesWithRemote<TestFunction, PaxosLong>> expected = ImmutableMap.<Long, PaxosResponsesWithRemote<TestFunction, PaxosLong>>builder()
                .put(1L, responsesFor(entry(node1, 2L)))
                .put(2L, responsesFor(entry(node1, 23L), entry(node2, 23L), entry(node3, 65L)))
                .put(5L, responsesFor(entry(node2, 32L)))
                .build();

        Map<Long, PaxosResponsesWithRemote<TestFunction, PaxosLong>> results =
                paxosQuorumChecker.apply(ImmutableSet.of(1L, 2L, 5L));
        assertThat(results).isEqualTo(expected);
    }

    private PaxosQuorumCheckingCoalescingFunction<Long, PaxosLong, TestFunction> paxosQuorumCheckerFor(
            TestFunction... nodes) {
        Map<TestFunction, ExecutorService> executors =
                Maps.asMap(ImmutableSet.copyOf(nodes), $ -> executorService);
        return new PaxosQuorumCheckingCoalescingFunction<>(
                ImmutableList.copyOf(nodes),
                executors,
                QUORUM_SIZE);
    }

    private static TestFunction functionFor(Map.Entry<Long, Long>... mappings) {
        Map<Long, Long> function = ImmutableMap.copyOf(ImmutableList.copyOf(mappings));
        return request -> KeyedStream.stream(function)
                .filterKeys(request::contains)
                .<PaxosLong>map(ImmutablePaxosLong::of)
                .collectToMap();
    }

    private interface TestFunction extends CoalescingRequestFunction<Long, PaxosLong> {}

    private static PaxosResponsesWithRemote<TestFunction, PaxosLong> responsesFor(Map.Entry<TestFunction, Long>... pairs) {
        Map<TestFunction, PaxosLong> mappings = KeyedStream.ofEntries(Arrays.stream(pairs))
                .map(PaxosLong::of)
                .collectToMap();

        return PaxosResponsesWithRemote.of(QUORUM_SIZE, mappings);
    }
}
