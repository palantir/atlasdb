/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

public final class LockClientTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testSerializationDeserialization() throws Exception {
        LockClient lockClient = new LockClient("foo");
        LockClient deserializedLockClient = mapper.readValue(mapper.writeValueAsString(lockClient), LockClient.class);

        assertThat(lockClient.getClientId(), is(deserializedLockClient.getClientId()));
        assertThat(lockClient.isAnonymous(), is(deserializedLockClient.isAnonymous()));
    }

}
