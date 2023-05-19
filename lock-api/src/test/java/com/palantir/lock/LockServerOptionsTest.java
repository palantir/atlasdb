/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class LockServerOptionsTest {
    private static final File LOCK_SERVER_OPTIONS_JSON = new File(IdentifiedTimeLockRequest.class
            .getResource("/lock-server-options.json")
            .getPath());
    private static final LockServerOptions NON_STANDALONE_OPTIONS =
            LockServerOptions.builder().isStandaloneServer(false).build();

    private final ObjectMapper serverMapper = ObjectMappers.newServerObjectMapper();
    private final ObjectMapper clientMapper = ObjectMappers.newClientObjectMapper();

    @Test
    public void serializationContainsIsStandaloneServerExplicitly() throws Exception {
        assertThat(serverMapper.writeValueAsString(NON_STANDALONE_OPTIONS))
                .isNotEmpty()
                .contains("\"isStandaloneServer\":false");
        assertThat(serverMapper.writeValueAsString(LockServerOptions.builder().build()))
                .isNotEmpty()
                .contains("\"isStandaloneServer\":true");
    }

    @Test
    public void serializationIsInverseOfDeserialization() throws JsonProcessingException {
        assertSerializationIsInverseOfDeserialization(NON_STANDALONE_OPTIONS);
        assertSerializationIsInverseOfDeserialization(
                LockServerOptions.builder().build());
    }

    @Test
    @SuppressWarnings("deprecation") // These are actual fields users might set that we still support...
    public void existingOptionsCanBeDeserialized() throws IOException {
        LockServerOptions lockServerOptions = LockServerOptions.builder()
                .maxAllowedLockTimeout(SimpleTimeDuration.of(1, TimeUnit.MILLISECONDS))
                .maxAllowedClockDrift(SimpleTimeDuration.of(2, TimeUnit.SECONDS))
                .maxAllowedBlockingDuration(SimpleTimeDuration.of(3, TimeUnit.MINUTES))
                .maxNormalLockAge(SimpleTimeDuration.of(4, TimeUnit.HOURS))
                .randomBitCount(32)
                .stuckTransactionTimeout(SimpleTimeDuration.of(5, TimeUnit.DAYS))
                .slowLogTriggerMillis(12345)
                .isStandaloneServer(false)
                .build();

        assertThat(clientMapper.readValue(LOCK_SERVER_OPTIONS_JSON, LockServerOptions.class))
                .isEqualTo(lockServerOptions);
        assertThat(serverMapper.readValue(LOCK_SERVER_OPTIONS_JSON, LockServerOptions.class))
                .isEqualTo(lockServerOptions);
    }

    private void assertSerializationIsInverseOfDeserialization(LockServerOptions options)
            throws JsonProcessingException {
        String json = serverMapper.writeValueAsString(options);
        assertThat(clientMapper.readValue(json, LockServerOptions.class)).isEqualTo(options);
        assertThat(serverMapper.readValue(json, LockServerOptions.class)).isEqualTo(options);
    }
}
