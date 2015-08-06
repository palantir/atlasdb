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
package com.palantir.timestamp.server;

import org.junit.ClassRule;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;
import com.palantir.timestamp.server.config.TimestampServerConfiguration;

import feign.Feign;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.jaxrs.JAXRSContract;
import io.dropwizard.testing.junit.DropwizardAppRule;
import io.dropwizard.testing.junit.DropwizardClientRule;

public class TimestampRemotingTest {
    @ClassRule
    public static final DropwizardAppRule<TimestampServerConfiguration> RULE = new DropwizardAppRule<>(TimestampServer.class, "src/test/resources/testService.yml");

    @ClassRule
    public final static DropwizardClientRule dropwizard = new DropwizardClientRule(new InMemoryTimestampService());

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

    @Test
    public void testSerializing() {
        ObjectMapper mapper = new ObjectMapper();

        String uri = dropwizard.baseUri().toString();
        TimestampService ts = Feign.builder()
                .decoder(new JacksonDecoder(mapper))
                .encoder(new JacksonEncoder(mapper))
                .contract(new JAXRSContract())
                .target(TimestampService.class, uri);

        long freshTimestamp = ts.getFreshTimestamp();
        TimestampRange freshTimestamps = ts.getFreshTimestamps(100);
    }

}
