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
package com.palantir.lock.logger;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.lock.HeldLocksGrant;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockCollections;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockServerConfigs;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.StringLockDescriptor;
import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class LockServiceSerDeTest {

    @Test
    public void testSerialisationAndDeserialisationOfLockResponse() throws Exception {
        HeldLocksToken token = LockServiceTestUtils.getFakeHeldLocksToken(
                "client A", "Fake thread", new BigInteger("1"), "held-lock-1", "logger-lock");
        LockResponse response = new LockResponse(token);
        ObjectMapper mapper = new ObjectMapper();
        LockResponse deserializedLockResponse =
                mapper.readValue(mapper.writeValueAsString(response), LockResponse.class);
        assertThat(response).isEqualTo(deserializedLockResponse);
    }

    @Test
    public void testSerialisationAndDeserialisationOfLockResponseWithLockHolders() throws Exception {
        HeldLocksToken token = LockServiceTestUtils.getFakeHeldLocksToken(
                "client A", "Fake thread", new BigInteger("1"), "held-lock-1", "logger-lock");
        Map<LockDescriptor, LockClient> lockHolders = ImmutableMap.of(
                StringLockDescriptor.of("lock_id"),
                LockClient.ANONYMOUS,
                StringLockDescriptor.of("lock_id2"),
                LockClient.of("client"));
        LockResponse response = new LockResponse(token, lockHolders);
        ObjectMapper mapper = new ObjectMapper();
        LockResponse deserializedLockResponse =
                mapper.readValue(mapper.writeValueAsString(response), LockResponse.class);
        assertThat(deserializedLockResponse.getLockHolders()).isEqualTo(lockHolders);
    }

    @Test
    public void testSerialisationAndDeserialisationOfDefaultLockServerOptions() throws Exception {
        LockServerOptions lockServerOptions = LockServerConfigs.DEFAULT;
        ObjectMapper mapper = new ObjectMapper();
        String serializedForm = mapper.writeValueAsString(lockServerOptions);
        LockServerOptions deserialzedLockServerOptions = mapper.readValue(serializedForm, LockServerOptions.class);
        assertThat(lockServerOptions).isEqualTo(deserialzedLockServerOptions);
    }

    @Test
    public void testSerialisationAndDeserialisationOfLockServerOptions() throws Exception {
        LockServerOptions lockServerOptions = LockServerOptions.builder()
                .isStandaloneServer(false)
                .slowLogTriggerMillis(10L)
                .build();
        ObjectMapper mapper = new ObjectMapper();
        String serializedForm = mapper.writeValueAsString(lockServerOptions);
        LockServerOptions deserialzedlockServerOptions = mapper.readValue(serializedForm, LockServerOptions.class);
        assertThat(deserialzedlockServerOptions).isEqualTo(lockServerOptions);
        assertThat(deserialzedlockServerOptions.isStandaloneServer()).isEqualTo(false);
        assertThat(deserialzedlockServerOptions.slowLogTriggerMillis()).isEqualTo(10L);
    }

    @Test
    public void testSerialisationAndDeserialisationOfHeldLocksGrant() throws Exception {
        ImmutableSortedMap<LockDescriptor, LockMode> lockDescriptorLockMode =
                LockServiceTestUtils.getLockDescriptorLockMode(ImmutableList.of("lock1", "lock2"));

        HeldLocksGrant heldLocksGrant = new HeldLocksGrant(
                BigInteger.ONE,
                System.currentTimeMillis(),
                System.currentTimeMillis() + 10L,
                LockCollections.of(lockDescriptorLockMode),
                SimpleTimeDuration.of(100, TimeUnit.SECONDS),
                10L);
        ObjectMapper mapper = new ObjectMapper();
        String serializedForm = mapper.writeValueAsString(heldLocksGrant);
        HeldLocksGrant deserialzedlockServerOptions = mapper.readValue(serializedForm, HeldLocksGrant.class);
        assertThat(deserialzedlockServerOptions).isEqualTo(heldLocksGrant);
    }

    @Test
    public void testPathParamSerDeOfLockClient() throws Exception {
        LockClient lockClient = LockClient.of("xyz");

        String serializedForm = lockClient.toString();
        LockClient lockClient1 = new LockClient(serializedForm);
        assertThat(lockClient1).isEqualTo(lockClient);
    }

    @Test
    public void testPathParamSerDeOfAnonymousLockClient() throws Exception {
        LockClient lockClient = LockClient.ANONYMOUS;

        String serializedForm = lockClient.toString();
        LockClient lockClient1 = new LockClient(serializedForm);
        assertThat(lockClient1).isEqualTo(lockClient);
    }
}
