package com.palantir.lock.server;

import org.junit.ClassRule;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.atlasdb.client.TextDelegateDecoder;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRequest;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.impl.LockServiceImpl;

import feign.Feign;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.jaxrs.JAXRSContract;
import io.dropwizard.testing.junit.DropwizardClientRule;

public class LockRemotingTest {
    @ClassRule
    public final static DropwizardClientRule lockService = new DropwizardClientRule(LockServiceImpl.create());

    @Test
    public void testLock() throws InterruptedException, JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();

        LockDescriptor desc = StringLockDescriptor.of("1234");
        LockRequest request = LockRequest.builder(ImmutableSortedMap.of(desc, LockMode.WRITE)).build();

        String writeValueAsString = mapper.writeValueAsString(request);

        RemoteLockService lock = Feign.builder()
                .decoder(new TextDelegateDecoder(new JacksonDecoder()))
                .encoder(new JacksonEncoder(mapper))
                .contract(new JAXRSContract())
                .target(RemoteLockService.class, lockService.baseUri().toString());



        lock.lockAnonymously(request);
    }
}
