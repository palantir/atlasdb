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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosValue;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LocalBatchPaxosLearnerTests {

    private static final Client CLIENT_1 = Client.of("client1");
    private static final Client CLIENT_2 = Client.of("client2");

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private LocalPaxosComponents paxosComponents;

    private BatchPaxosLearner resource;

    @Before
    public void before() {
        resource = new LocalBatchPaxosLearner(paxosComponents);
    }

    @Test
    public void weProxyLearnRequestsThrough() {
        PaxosValue paxosValue1 = paxosValue(1);
        PaxosValue paxosValue2 = paxosValue(2);

        SetMultimap<Client, PaxosValue> learnData = ImmutableSetMultimap.<Client, PaxosValue>builder()
                .putAll(CLIENT_1, paxosValue1)
                .putAll(CLIENT_2, paxosValue1, paxosValue2)
                .build();

        resource.learn(learnData);

        verify(paxosComponents.learner(CLIENT_1)).learn(1, paxosValue1);
        verify(paxosComponents.learner(CLIENT_2)).learn(1, paxosValue1);
        verify(paxosComponents.learner(CLIENT_2)).learn(2, paxosValue2);
    }

    @Test
    public void weProxyGetLearnedValuesThrough() {
        PaxosValue paxosValue1 = paxosValue(1);
        PaxosValue paxosValue2 = paxosValue(2);
        when(paxosComponents.learner(CLIENT_1).getLearnedValue(1)).thenReturn(Optional.of(paxosValue1));
        when(paxosComponents.learner(CLIENT_1).getLearnedValue(2)).thenReturn(Optional.empty());
        when(paxosComponents.learner(CLIENT_2).getLearnedValue(1)).thenReturn(Optional.of(paxosValue1));
        when(paxosComponents.learner(CLIENT_2).getLearnedValue(2)).thenReturn(Optional.of(paxosValue2));

        SetMultimap<Client, PaxosValue> expected = ImmutableSetMultimap.<Client, PaxosValue>builder()
                .putAll(CLIENT_1, paxosValue1)
                .putAll(CLIENT_2, paxosValue1, paxosValue2)
                .build();

        Set<WithSeq<Client>> request = ImmutableSet.of(
                WithSeq.of(CLIENT_1, 1), WithSeq.of(CLIENT_1, 2), WithSeq.of(CLIENT_2, 1), WithSeq.of(CLIENT_2, 2));

        assertThat(resource.getLearnedValues(request)).isEqualTo(expected);
    }

    @Test
    public void weProxyGetLearnedValuesSince() {
        PaxosValue paxosValue1 = paxosValue(1);
        PaxosValue paxosValue2 = paxosValue(2);
        when(paxosComponents.learner(CLIENT_1).getLearnedValuesSince(1)).thenReturn(ImmutableSet.of(paxosValue1));
        when(paxosComponents.learner(CLIENT_2).getLearnedValuesSince(2)).thenReturn(ImmutableSet.of(paxosValue2));

        Map<Client, Long> request = ImmutableMap.<Client, Long>builder()
                .put(CLIENT_1, 1L)
                .put(CLIENT_2, 2L)
                .build();

        SetMultimap<Client, PaxosValue> expected = ImmutableSetMultimap.<Client, PaxosValue>builder()
                .putAll(CLIENT_1, paxosValue1)
                .putAll(CLIENT_2, paxosValue2)
                .build();

        assertThat(resource.getLearnedValuesSince(request)).isEqualTo(expected);
    }

    private static PaxosValue paxosValue(long round) {
        return new PaxosValue(UUID.randomUUID().toString(), round, null);
    }
}
