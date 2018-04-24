/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.timelock.paxos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.net.HostAndPort;
import com.palantir.leader.LeaderElectionService;

import io.dropwizard.jackson.Jackson;

public class PaxosLeaderElectionServiceSerializationTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    static {
        MAPPER.registerModule(new GuavaModule());
        MAPPER.registerModule(new Jdk8Module());
    }

    @Test
    public void canSerializeNoSuspectedLeader() throws JsonProcessingException {
        LeaderElectionService leaderElectionService = mock(LeaderElectionService.class);
        when(leaderElectionService.getSuspectedLeaderInMemory()).thenReturn(Optional.empty());

        // Be very careful about changing the following! Doing so would be a wire break.
        assertThat(MAPPER.writeValueAsString(leaderElectionService.getSuspectedLeaderInMemory())).isEqualTo("null");
    }

    @Test
    public void canSerializeSuspectedLeader() throws JsonProcessingException {
        Optional<HostAndPort> suspectedLeader = Optional.of(HostAndPort.fromParts("foo", 123));

        LeaderElectionService leaderElectionService = mock(LeaderElectionService.class);
        when(leaderElectionService.getSuspectedLeaderInMemory()).thenReturn(suspectedLeader);

        // Be very careful about changing the following! Doing so would be a wire break.
        assertThat(MAPPER.writeValueAsString(leaderElectionService.getSuspectedLeaderInMemory()))
                .isEqualTo("\"foo:123\"");
    }
}
