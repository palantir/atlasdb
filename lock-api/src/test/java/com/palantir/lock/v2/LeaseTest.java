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

package com.palantir.lock.v2;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.palantir.common.time.NanoTime;

public class LeaseTest {
    private static final LeaderTime leaderTime = LeaderTime.of(LeadershipId.random(), NanoTime.createForTests(10L));
    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Test
    public void serializeDeserialize() throws Exception {
        Lease lease = Lease.of(leaderTime, Duration.ZERO);
        String serialized = objectMapper.writeValueAsString(lease);
        System.out.println(serialized);
        Lease deserialized = objectMapper.readValue(serialized, Lease.class);

        assertThat(deserialized).isEqualTo(lease);
    }

    @Test
    public void serializeDeserialize2() throws Exception {
        Lease lease = Lease.of(LeadershipId.random(), NanoTime.createForTests(1), Duration.ZERO);
        String serialized = objectMapper.writeValueAsString(lease);
        System.out.println(serialized);
        Lease deserialized = objectMapper.readValue(serialized, Lease.class);

        assertThat(deserialized).isEqualTo(lease);
    }
}