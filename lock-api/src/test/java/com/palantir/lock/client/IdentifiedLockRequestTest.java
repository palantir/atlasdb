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

package com.palantir.lock.client;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.junit.Test;

public class IdentifiedLockRequestTest {
    private static final String SERIALIZED_LOCK_REQUEST = "{"
            + "\"requestId\":\"885afd9c-de62-44ff-a517-5db14b71bfaa\","
            + "\"lockDescriptors\":[{\"bytes\":\"Zm9v\"}],"
            + "\"acquireTimeoutMs\":123,"
            + "\"clientDescription\":\"Thread: main\"}";

    private static final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new Jdk8Module())
            .registerModule(new GuavaModule());

    @Test
    public void ensureSerdeBackcompat() throws Exception {
        IdentifiedLockRequest request = mapper.readValue(SERIALIZED_LOCK_REQUEST, IdentifiedLockRequest.class);
        String deserialized = mapper.writeValueAsString(request);
        assertThat(mapper.readTree(deserialized)).isEqualTo(mapper.readTree(SERIALIZED_LOCK_REQUEST));
    }
}
