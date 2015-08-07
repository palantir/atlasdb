package com.palantir.lock.server;

import java.io.IOException;

import org.junit.ClassRule;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.atlasdb.client.TextDelegateDecoder;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.impl.LockServiceImpl;

import feign.Feign;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.jaxrs.JAXRSContract;
import io.dropwizard.testing.junit.DropwizardClientRule;

public class LockRemotingTest {
    static LockServiceImpl rawLock = LockServiceImpl.create();

    @ClassRule
    public final static DropwizardClientRule lockService = new DropwizardClientRule(rawLock);

    @Test
    public void testLock() throws InterruptedException, IOException {
        ObjectMapper mapper = new ObjectMapper();

        LockDescriptor desc = StringLockDescriptor.of("1234");
        String writeValueAsString = mapper.writeValueAsString(desc);
        System.out.println(writeValueAsString);
        LockDescriptor desc2 = mapper.readValue(writeValueAsString, LockDescriptor.class);

        LockRequest request = LockRequest.builder(ImmutableSortedMap.of(desc, LockMode.WRITE)).build();
        writeValueAsString = mapper.writeValueAsString(request);
        System.out.println(writeValueAsString);
        LockRequest request2 = mapper.readValue(writeValueAsString, LockRequest.class);

        LockRefreshToken lockResponse = rawLock.lockAnonymously(request);
        rawLock.unlock(lockResponse);
        writeValueAsString = mapper.writeValueAsString(lockResponse);
        System.out.println(writeValueAsString);
        LockResponse lockResponse2 = mapper.readValue(writeValueAsString, LockResponse.class);

        RemoteLockService lock = Feign.builder()
                .decoder(new TextDelegateDecoder(new JacksonDecoder()))
                .encoder(new JacksonEncoder(mapper))
                .contract(new JAXRSContract())
                .target(RemoteLockService.class, lockService.baseUri().toString());


        lock.lockAnonymously(request);
    }
}
