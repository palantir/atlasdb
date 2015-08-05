package com.palantir.timestamp.server;

import org.junit.ClassRule;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;
import com.palantir.timestamp.server.config.TimestampServerConfiguration;

import feign.Feign;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.jaxrs.JAXRSContract;
import io.dropwizard.testing.junit.DropwizardAppRule;

public class RemotingTest {
    @ClassRule
    public static final DropwizardAppRule<TimestampServerConfiguration> RULE = new DropwizardAppRule<>(TimestampServer.class, "src/test/resources/testService.yml");


    @Test
    public void testRemoting() {
        ObjectMapper mapper = new ObjectMapper();

        TimestampService ts = Feign.builder()
                .decoder(new JacksonDecoder(mapper))
                .encoder(new JacksonEncoder(mapper))
                .contract(new JAXRSContract())
                .target(TimestampService.class, "http://localhost:" + RULE.getLocalPort());

        long freshTimestamp = ts.getFreshTimestamp();
        TimestampRange freshTimestamps = ts.getFreshTimestamps(100);
    }

}
