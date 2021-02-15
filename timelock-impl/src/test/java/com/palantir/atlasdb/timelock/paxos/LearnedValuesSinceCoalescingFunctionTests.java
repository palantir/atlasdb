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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosUpdate;
import com.palantir.paxos.PaxosValue;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LearnedValuesSinceCoalescingFunctionTests {

    private static final Client CLIENT_1 = Client.of("client-1");
    private static final Client CLIENT_2 = Client.of("client-2");
    private static final Client CLIENT_3 = Client.of("client-3");

    @Mock(answer = Answers.RETURNS_SMART_NULLS)
    private BatchPaxosLearner remote;

    @Test
    public void batchesAndReturnsPreciselyValuesLearnedSinceSeq() {
        PaxosValue paxosValue1 = paxosValue(10);
        PaxosValue paxosValue2 = paxosValue(12);
        PaxosValue paxosValue3 = paxosValue(20);
        PaxosValue paxosValue4 = paxosValue(25);

        Set<WithSeq<Client>> request = ImmutableSet.of(
                WithSeq.of(CLIENT_1, 10),
                WithSeq.of(CLIENT_1, 12),
                WithSeq.of(CLIENT_1, 14),
                WithSeq.of(CLIENT_2, 15),
                WithSeq.of(CLIENT_2, 23),
                WithSeq.of(CLIENT_3, 1));

        // we pick the lowest sequence number from the set per client
        Map<Client, Long> minimumRequest = ImmutableMap.<Client, Long>builder()
                .put(CLIENT_1, 10L)
                .put(CLIENT_2, 15L)
                .put(CLIENT_3, 1L)
                .build();

        SetMultimap<Client, PaxosValue> remoteResponse = ImmutableSetMultimap.<Client, PaxosValue>builder()
                .putAll(CLIENT_1, paxosValue1, paxosValue2, paxosValue4)
                .putAll(CLIENT_2, paxosValue3, paxosValue4)
                .build();

        when(remote.getLearnedValuesSince(minimumRequest)).thenReturn(remoteResponse);

        LearnedValuesSinceCoalescingFunction function = new LearnedValuesSinceCoalescingFunction(remote);
        SetMultimap<WithSeq<Client>, PaxosValue> asMultimap = KeyedStream.stream(function.apply(request))
                .map(PaxosUpdate::getValues)
                .flatMap(Collection::stream)
                .collectToSetMultimap();

        SetMultimap<WithSeq<Client>, PaxosValue> expectedResult =
                ImmutableSetMultimap.<WithSeq<Client>, PaxosValue>builder()
                        .putAll(WithSeq.of(CLIENT_1, 10), paxosValue1, paxosValue2, paxosValue4)
                        .putAll(WithSeq.of(CLIENT_1, 12), paxosValue2, paxosValue4)
                        .putAll(WithSeq.of(CLIENT_1, 14), paxosValue4)
                        .putAll(WithSeq.of(CLIENT_2, 15), paxosValue3, paxosValue4)
                        .putAll(WithSeq.of(CLIENT_2, 23), paxosValue4)
                        .build();

        assertThat(asMultimap).isEqualTo(expectedResult);

        assertThat(asMultimap.keySet())
                .as("despite requesting learnt values for client-3, we still learn nothing, if remote server hasn't "
                        + "learnt anything for that client")
                .doesNotContain(WithSeq.of(CLIENT_3, 1));
    }

    private static PaxosValue paxosValue(long round) {
        return new PaxosValue(UUID.randomUUID().toString(), round, new byte[] {0});
    }
}
