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

import java.util.UUID;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class LeadershipIdTest {
    private static final UUID UUID_1 = UUID.randomUUID();
    private static final UUID UUID_2 = UUID.randomUUID();

    @Test
    public void testEquality() {
        LeadershipId id_1 = new LeadershipId(UUID_1);
        LeadershipId id_2 = new LeadershipId(UUID_1);
        LeadershipId id_3 = new LeadershipId(UUID_2);

        assertThat(id_1).isEqualTo(id_2);
        assertThat(id_1).isNotEqualTo(id_3);
    }

    @Test
    public void serializeAndDeserializeSuccessfully() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        LeadershipId leadershipId = LeadershipId.random();
        String serialized = mapper.writeValueAsString(leadershipId);
        LeadershipId deserialized = mapper.readValue(serialized, LeadershipId.class);

        assertThat(deserialized).isEqualTo(leadershipId);
    }
}
