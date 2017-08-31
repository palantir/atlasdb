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
package com.palantir.lock.logger;

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;
import java.util.Map;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.StringLockDescriptor;

public class LockServiceSerDeTest {

    @Test
    public void testSerialisationAndDeserialisationOfLockResponse() throws Exception {
        HeldLocksToken token = LockServiceTestUtils.getFakeHeldLocksToken("client A", "Fake thread",
                new BigInteger("1"), "held-lock-1",
                "logger-lock");
        LockResponse response = new LockResponse(token);
        ObjectMapper mapper = new ObjectMapper();
        LockResponse deserializedLockResponse = mapper.readValue(mapper.writeValueAsString(response),
                LockResponse.class);
        assertEquals(deserializedLockResponse, response);
    }

    @Test
    public void testSerialisationAndDeserialisationOfLockResponseWithLockHolders() throws Exception {
        HeldLocksToken token = LockServiceTestUtils.getFakeHeldLocksToken("client A", "Fake thread",
                new BigInteger("1"), "held-lock-1",
                "logger-lock");
        Map<LockDescriptor, LockClient> lockHolders = ImmutableMap.of(StringLockDescriptor.of("lockdid"),
                LockClient.ANONYMOUS);
        LockResponse response = new LockResponse(token, lockHolders);
        ObjectMapper mapper = new ObjectMapper();
        LockResponse deserializedLockResponse = mapper.readValue(mapper.writeValueAsString(response),
                LockResponse.class);
        assertEquals(deserializedLockResponse, response);
    }

    @Test
    public void testSerialisationAndDeserialisationOfDefaultLockServerOptions() throws Exception {
        LockServerOptions lockServerOptions = LockServerOptions.DEFAULT;
        ObjectMapper mapper = new ObjectMapper();
        String serializedForm = mapper.writeValueAsString(lockServerOptions);
        LockServerOptions deserialzedLockServerOptions = mapper.readValue(serializedForm, LockServerOptions.class);
        assertEquals(deserialzedLockServerOptions, lockServerOptions);
    }

    @Test
    public void testSerialisationAndDeserialisationOfLockServerOptions() throws Exception {
        LockServerOptions lockServerOptions = new LockServerOptions.Builder()
                .standaloneServer(false)
                .slowLogTriggerMillis(10L)
                .build();
        ObjectMapper mapper = new ObjectMapper();
        String serializedForm = mapper.writeValueAsString(lockServerOptions);
        LockServerOptions deserialzedlockServerOptions = mapper.readValue(serializedForm, LockServerOptions.class);
        assertEquals(lockServerOptions, deserialzedlockServerOptions);
        assertEquals(false, deserialzedlockServerOptions.isStandaloneServer());
        assertEquals(10L, deserialzedlockServerOptions.slowLogTriggerMillis());
    }
}
