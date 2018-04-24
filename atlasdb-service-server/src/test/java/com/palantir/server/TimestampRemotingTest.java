/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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

import java.util.Random;

import org.junit.ClassRule;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.atlasdb.http.NotCurrentLeaderExceptionMapper;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;

import feign.Feign;
import feign.Retryer;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.jaxrs.JAXRSContract;
import io.dropwizard.testing.junit.DropwizardClientRule;

public class TimestampRemotingTest {
    @ClassRule
    public final static DropwizardClientRule dropwizard = new DropwizardClientRule(new InMemoryTimestampService());

    public final static Random rand = new Random(0);

    @ClassRule
    public final static DropwizardClientRule notLeader = new DropwizardClientRule(new TimestampService() {
        @Override
        public long getFreshTimestamp() {
            if (rand.nextBoolean()) {
                throw new NotCurrentLeaderException("not the leader");
            } else {
                return 0;
            }
        }

        @Override
        public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
            throw new NotCurrentLeaderException("not the leader");
        }
    }, new NotCurrentLeaderExceptionMapper());

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

    @Test
    public void testNotLeader() {
        ObjectMapper mapper = new ObjectMapper();
        String uri = notLeader.baseUri().toString();
        TimestampService ts = Feign.builder()
                .decoder(new JacksonDecoder(mapper))
                .encoder(new JacksonEncoder(mapper))
                .contract(new JAXRSContract())
                // we try up to 40 times with .5 chance.  This means this test will strobe once in ~10^12 times
                .retryer(new Retryer.Default(1, 1, 40))
                .target(TimestampService.class, notLeader.baseUri().toString());

        ts.getFreshTimestamp();
    }

}
