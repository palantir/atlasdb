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

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.junit.ClassRule;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;
import com.palantir.timestamp.server.NotCurrentLeaderExceptionMapper;

import feign.Feign;
import feign.Response;
import feign.Response.Body;
import feign.codec.ErrorDecoder;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.jaxrs.JAXRSContract;
import io.dropwizard.testing.junit.DropwizardClientRule;

public class TimestampRemotingTest {
    @ClassRule
    public final static DropwizardClientRule dropwizard = new DropwizardClientRule(new InMemoryTimestampService());

    @ClassRule
    public final static DropwizardClientRule notLeader = new DropwizardClientRule(new TimestampService() {
        @Override
        public long getFreshTimestamp() {
            throw new NotCurrentLeaderException("not the leader");
        }

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
        ErrorDecoder errorDecoder = new ErrorDecoder() {
            @Override
            public Exception decode(String methodKey, Response response) {
                Body body = response.body();
                try {
                    String string = IOUtils.toString(body.asInputStream());
                    System.out.println(string);
                } catch (IOException e) {
                    return e;
                }
                return null;
            }
        };

        //TODO: add RequestInterceptor to handle inbox and outbox for out of band params
        String uri = notLeader.baseUri().toString();
        TimestampService ts = Feign.builder()
                .decoder(new JacksonDecoder(mapper))
                .encoder(new JacksonEncoder(mapper))
                .contract(new JAXRSContract())
//                .errorDecoder(errorDecoder)
//                .client(null)
//                .target(TimestampService.class, "http://localhost:1234");
                .target(TimestampService.class, notLeader.baseUri().toString());

        ts.getFreshTimestamp();
    }

}
