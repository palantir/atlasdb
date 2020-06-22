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

import com.google.common.collect.ImmutableSet;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosValue;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BatchPingableLeaderResourceTests {

    private static final Client CLIENT_1 = Client.of("client1");
    private static final Client CLIENT_2 = Client.of("client2");

    private static final UUID LEADER_UUID = UUID.randomUUID();

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private LocalPaxosComponents components;
    private BatchPingableLeaderResource resource;

    @Before
    public void before() {
        resource = new BatchPingableLeaderResource(LEADER_UUID, components);
    }

    @Test
    public void pingReturnsTrueForLeaders() {
        when(components.learner(CLIENT_1).getGreatestLearnedValue())
                .thenReturn(paxosValue(LEADER_UUID));
        when(components.learner(CLIENT_2).getGreatestLearnedValue())
                .thenReturn(paxosValue(LEADER_UUID));

        assertThat(resource.ping(ImmutableSet.of(CLIENT_1, CLIENT_2)))
                .containsOnly(CLIENT_1, CLIENT_2);
    }

    @Test
    public void pingFiltersOutNonLeaders() {
        Client clientLedByAnotherServer = Client.of("client-led-by-another-server");

        when(components.learner(CLIENT_1).getGreatestLearnedValue())
                .thenReturn(paxosValue(LEADER_UUID));
        when(components.learner(clientLedByAnotherServer).getGreatestLearnedValue())
                .thenReturn(paxosValue(UUID.randomUUID()));

        assertThat(resource.ping(ImmutableSet.of(CLIENT_1, clientLedByAnotherServer)))
                .containsOnly(CLIENT_1);
    }

    @Test
    public void filtersOutClientsWhereNothingHasBeenLearnt() {
        Client clientWhereNothingHasBeenLearnt = Client.of("client-where-nothing-has-been-learnt");

        when(components.learner(CLIENT_1).getGreatestLearnedValue())
                .thenReturn(paxosValue(LEADER_UUID));
        when(components.learner(clientWhereNothingHasBeenLearnt).getGreatestLearnedValue())
                .thenReturn(Optional.empty());

        assertThat(resource.ping(ImmutableSet.of(CLIENT_1, clientWhereNothingHasBeenLearnt)))
                .containsOnly(CLIENT_1);
    }

    private static Optional<PaxosValue> paxosValue(UUID uuid) {
        return Optional.of(new PaxosValue(uuid.toString(), new Random().nextLong(), null));
    }
}
