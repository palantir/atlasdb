/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.server;

import static org.hamcrest.Matchers.hasSize;

import java.io.IOException;
import java.util.Set;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.atlasdb.http.TextDelegateDecoder;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.SortedLockCollection;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.impl.LockServiceImpl;

import feign.Feign;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.jaxrs.JAXRSContract;
import io.dropwizard.testing.junit.DropwizardClientRule;

public class LockRemotingTest {
    private static final ObjectMapper mapper = new ObjectMapper();
    
    private static LockServiceImpl rawLock = LockServiceImpl.create();

    @ClassRule
    public final static DropwizardClientRule lockService = new DropwizardClientRule(rawLock);
    
    private RemoteLockService lock = Feign.builder()
            .decoder(new TextDelegateDecoder(new JacksonDecoder()))
            .encoder(new JacksonEncoder(mapper))
            .contract(new JAXRSContract())
            .target(RemoteLockService.class, lockService.baseUri().toString());
    
    @Test
    public void testLock() throws InterruptedException, IOException {
        LockDescriptor desc = StringLockDescriptor.of("1234");
        String writeValueAsString = mapper.writeValueAsString(desc);
        LockDescriptor desc2 = mapper.readValue(writeValueAsString, LockDescriptor.class);

        long minVersion = 123;
        LockRequest request = LockRequest.builder(ImmutableSortedMap.of(desc, LockMode.WRITE))
                .doNotBlock()
                .withLockedInVersionId(minVersion)
                .build();
        
        writeValueAsString = mapper.writeValueAsString(request);
        LockRequest request2 = mapper.readValue(writeValueAsString, LockRequest.class);

        LockRefreshToken lockResponse = rawLock.lock(LockClient.ANONYMOUS.getClientId(), request);
        rawLock.unlock(lockResponse);
        writeValueAsString = mapper.writeValueAsString(lockResponse);
        LockRefreshToken lockResponse2 = mapper.readValue(writeValueAsString, LockRefreshToken.class);

        String lockClient = "23234";
        LockRefreshToken token = lock.lock(lockClient, request);
        long minLockedInVersionId = lock.getMinLockedInVersionId(lockClient);
        Assert.assertEquals(minVersion, minLockedInVersionId);
        lock.unlock(token);
        token = lock.lock(LockClient.ANONYMOUS.getClientId(), request);
        Set<LockRefreshToken> refreshed = lock.refreshLockRefreshTokens(ImmutableList.of(token));
        Assert.assertEquals(1, refreshed.size());
        lock.unlock(token);
        lock.logCurrentState();
        lock.currentTimeMillis();
    }
    
    @Test
    public void testLockAndGetHeldLocks() throws InterruptedException {
        String client1 = "client1";
        String client2 = "client2";
        
        LockDescriptor desc1 = StringLockDescriptor.of("1234");
        LockDescriptor desc2 = StringLockDescriptor.of("4567");
        
        long minVersion = 123;
        
        LockRequest request = LockRequest.builder(ImmutableSortedMap.of(desc1, LockMode.WRITE))
                .doNotBlock()
                .withLockedInVersionId(minVersion)
                .build();
        
        HeldLocksToken heldLocksToken1 = lock.lockAndGetHeldLocks(client1, request);
        SortedLockCollection<LockDescriptor> locks1 = heldLocksToken1.getLocks();
        
        Assert.assertThat(locks1.getKeys(), hasSize(1));       
        Assert.assertThat(locks1.getKeys().get(0).getLockIdAsString(), Matchers.is(desc1.getLockIdAsString()));
        Assert.assertThat(locks1.getKeys().get(0), Matchers.is(desc1));
        
        LockRequest request2 = LockRequest.builder(ImmutableSortedMap.of(
                desc1, LockMode.WRITE, desc2, LockMode.READ))
                .doNotBlock()
                .withLockedInVersionId(minVersion)
                .build();
        
        HeldLocksToken heldLocksToken2 = lock.lockAndGetHeldLocks(client2, request2);
        SortedLockCollection<LockDescriptor> locks2 = heldLocksToken2.getLocks();
        
        Assert.assertThat(locks2.getKeys(), Matchers.hasSize(1));
    }
}
