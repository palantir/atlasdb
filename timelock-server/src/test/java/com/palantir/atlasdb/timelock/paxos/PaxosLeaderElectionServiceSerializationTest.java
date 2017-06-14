/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import static org.assertj.core.api.Java6Assertions.assertThat;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.base.Optional;
import com.google.common.net.HostAndPort;

import io.dropwizard.jackson.Jackson;

public class PaxosLeaderElectionServiceSerializationTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    static {
        MAPPER.registerModule(new GuavaModule());
        MAPPER.registerModule(new Jdk8Module());
    }

    @Test
    public void canSerializeNoSuspectedLeader() throws JsonProcessingException {
        Optional<HostAndPort> noSuspectedLeader = Optional.absent();

        // Be very careful about changing the following! Doing so would be a wire break.
        assertThat(MAPPER.writeValueAsString(noSuspectedLeader)).isEqualTo("null");
    }

    @Test
    public void canSerializeSuspectedLeader() throws JsonProcessingException {
        Optional<HostAndPort> suspectedLeader = Optional.of(HostAndPort.fromParts("foo", 123));

        // Be very careful about changing the following! Doing so would be a wire break.
        assertThat(MAPPER.writeValueAsString(suspectedLeader)).isEqualTo("\"foo:123\"");
    }
}
