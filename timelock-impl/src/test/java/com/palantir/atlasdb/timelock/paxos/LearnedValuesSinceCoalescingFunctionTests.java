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
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.PaxosValue;

@RunWith(MockitoJUnitRunner.class)
public class LearnedValuesSinceCoalescingFunctionTests {

    private static final Client CLIENT_1 = Client.of("client-1");
    private static final Client CLIENT_2 = Client.of("client-2");

    @Mock(answer = Answers.RETURNS_SMART_NULLS)
    private BatchPaxosLearner remote;

    @Test
    public void canProcessBatch() {
        PaxosValue paxosValue1 = paxosValue(10);
        PaxosValue paxosValue2 = paxosValue(12);
        PaxosValue paxosValue3 = paxosValue(20);
        PaxosValue paxosValue4 = paxosValue(25);

        Set<WithSeq<Client>> request = ImmutableSet.of(
                WithSeq.of(10, CLIENT_1),
                WithSeq.of(12, CLIENT_1),
                WithSeq.of(14, CLIENT_1),
                WithSeq.of(15, CLIENT_2),
                WithSeq.of(23, CLIENT_2));

        // we pick the lowest sequence number from the set per client
        Map<Client, Long> minimumRequest = ImmutableMap.<Client, Long>builder()
                .put(CLIENT_1, 10L)
                .put(CLIENT_2, 15L)
                .build();

        SetMultimap<Client, PaxosValue> remoteResponse = ImmutableSetMultimap.<Client, PaxosValue>builder()
                .putAll(CLIENT_1, paxosValue1, paxosValue2, paxosValue4)
                .putAll(CLIENT_2, paxosValue3, paxosValue4)
                .build();

        when(remote.getLearnedValuesSince(minimumRequest))
                .thenReturn(remoteResponse);

        LearnedValuesSinceCoalescingFunction function = new LearnedValuesSinceCoalescingFunction(remote);
        Map<WithSeq<Client>, Collection<PaxosValue>> results = function.apply(request);
        SetMultimap<WithSeq<Client>, PaxosValue> asMultimap = KeyedStream.stream(results)
                .flatMap(Collection::stream)
                .collectToSetMultimap();

        SetMultimap<WithSeq<Client>, PaxosValue> expectedResult =
                ImmutableSetMultimap.<WithSeq<Client>, PaxosValue>builder()
                        .putAll(WithSeq.of(10, CLIENT_1), paxosValue1, paxosValue2, paxosValue4)
                        .putAll(WithSeq.of(12, CLIENT_1), paxosValue2, paxosValue4)
                        .putAll(WithSeq.of(14, CLIENT_1), paxosValue4)
                        .putAll(WithSeq.of(15, CLIENT_2), paxosValue3, paxosValue4)
                        .putAll(WithSeq.of(23, CLIENT_2), paxosValue4)
                        .build();

        assertThat(asMultimap).isEqualTo(expectedResult);
    }

    private static PaxosValue paxosValue(long round) {
        return new PaxosValue(UUID.randomUUID().toString(), round, new byte[] {0});
    }
}
