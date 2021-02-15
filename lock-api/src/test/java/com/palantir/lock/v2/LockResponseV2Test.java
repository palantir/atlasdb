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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.palantir.common.time.NanoTime;
import java.time.Duration;
import java.util.UUID;
import org.junit.Test;

public class LockResponseV2Test {
    private static final LockToken LOCK_TOKEN = LockToken.of(UUID.randomUUID());
    private static final RuntimeException EXCEPTION = new RuntimeException("failure!");
    private static final Lease LEASE =
            Lease.of(LeaderTime.of(LeadershipId.random(), NanoTime.createForTests(10)), Duration.ofSeconds(1));

    private static final String SUCCESSFUL_LOCK_RESPONSE = "{"
            + "\"type\":\"success\","
            + "\"token\":{\"requestId\":\"803a09b5-97cb-4034-b35e-1db299b93a7e\"},"
            + "\"lease\":{"
            + "\"leaderTime\":{\"id\":\"2600e8ef-cdb5-441e-a235-475465b7b7fa\","
            + "\"currentTime\":10},"
            + "\"validity\":1.000000000}}";

    private static final String UNSUCCESSFUL_LOCK_RESPONSE = "{\"type\":\"failure\"}";

    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Test
    public void visitsSuccessfulResponse() {
        LockResponseV2 response = LockResponseV2.successful(LOCK_TOKEN, LEASE);
        LockToken token =
                response.accept(LockResponseV2.Visitor.of(LockResponseV2.Successful::getToken, unsuccessful -> {
                    throw EXCEPTION;
                }));

        assertThat(token).isEqualTo(LOCK_TOKEN);
    }

    @Test
    public void visitsUnsuccessfulResponse() {
        LockResponseV2 response = LockResponseV2.timedOut();

        assertThatThrownBy(() ->
                        response.accept(LockResponseV2.Visitor.of(LockResponseV2.Successful::getToken, unsuccessful -> {
                            throw EXCEPTION;
                        })))
                .isEqualTo(EXCEPTION);
    }

    @Test
    public void serializeDeserialize_Successful() throws Exception {
        LockResponseV2 lockResponse = objectMapper.readValue(SUCCESSFUL_LOCK_RESPONSE, LockResponseV2.class);
        assertThat(objectMapper.writeValueAsString(lockResponse)).isEqualTo(SUCCESSFUL_LOCK_RESPONSE);
    }

    @Test
    public void serializeDeserialize_Unsuccessful() throws Exception {
        LockResponseV2 lockResponse = objectMapper.readValue(UNSUCCESSFUL_LOCK_RESPONSE, LockResponseV2.class);
        assertThat(objectMapper.writeValueAsString(lockResponse)).isEqualTo(UNSUCCESSFUL_LOCK_RESPONSE);
    }
}
