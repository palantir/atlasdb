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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.palantir.common.time.NanoTime;
import java.time.Duration;
import org.junit.Test;

public class LeaseTest {
    private static final LeadershipId LEADERSHIP_ID_1 = LeadershipId.random();
    private static final LeadershipId LEADERSHIP_ID_2 = LeadershipId.random();
    private static final LeaderTime leaderTime = LeaderTime.of(LEADERSHIP_ID_1, NanoTime.createForTests(10));
    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    private static final String SERIALIZED_LEASE = "{"
            + "\"leaderTime\":{"
                + "\"id\":\"190d1ed6-824b-465a-b3dc-7401b23ace68\","
                + "\"currentTime\":10},"
            + "\"validity\":1.000000000}";

    @Test
    public void shouldBeValidAfterCreation() {
        Lease lease = Lease.of(LeaderTime.of(LEADERSHIP_ID_1, time(10)),
                Duration.ofNanos(20));

        assertThat(lease.isValid(LeaderTime.of(LEADERSHIP_ID_1, time(10)))).isTrue();
    }

    @Test
    public void shouldBeValidBeforeExpiration() {
        Lease lease = Lease.of(LeaderTime.of(LEADERSHIP_ID_1, time(10)),
                Duration.ofNanos(20));

        assertThat(lease.isValid(LeaderTime.of(LEADERSHIP_ID_1, time(10 + 19)))).isTrue();
    }

    @Test
    public void shouldBeInvalidAfterExpiration() {
        Lease lease = Lease.of(LeaderTime.of(LEADERSHIP_ID_1, time(10)),
                Duration.ofNanos(20));

        assertThat(lease.isValid(LeaderTime.of(LEADERSHIP_ID_1, time(10 + 21)))).isFalse();
    }

    @Test
    public void shouldBeInvalidRegardlessOfTimeIfLeaderIsChanged() {
        Lease lease = Lease.of(LeaderTime.of(LEADERSHIP_ID_1, time(10)),
                Duration.ofNanos(20L));

        assertThat(lease.isValid(LeaderTime.of(LEADERSHIP_ID_2, time(0)))).isFalse();
        assertThat(lease.isValid(LeaderTime.of(LEADERSHIP_ID_2, time(10)))).isFalse();
        assertThat(lease.isValid(LeaderTime.of(LEADERSHIP_ID_2, time(10 + 21)))).isFalse();
    }

    @Test
    public void canBeSerializedAndDeserialize() throws Exception {
        Lease deserialized = objectMapper.readValue(SERIALIZED_LEASE, Lease.class);
        String serialized = objectMapper.writeValueAsString(deserialized);

        assertThat(serialized).isEqualTo(SERIALIZED_LEASE);
    }

    private NanoTime time(long nanos) {
        return NanoTime.createForTests(nanos);
    }
}
