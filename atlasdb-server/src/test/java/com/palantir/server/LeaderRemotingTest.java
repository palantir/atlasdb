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
import java.util.UUID;

import org.junit.ClassRule;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.atlasdb.http.TextDelegateDecoder;
import com.palantir.leader.PingableLeader;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosAcceptorImpl;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerImpl;
import com.palantir.paxos.PaxosProposal;
import com.palantir.paxos.PaxosProposalId;
import com.palantir.paxos.PaxosValue;

import feign.Feign;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.jaxrs.JAXRSContract;
import io.dropwizard.testing.junit.DropwizardClientRule;

public class LeaderRemotingTest {
    @ClassRule
    public final static DropwizardClientRule pingable = new DropwizardClientRule(new PingableLeader() {
        @Override
        public boolean ping() {
            return true;
        }

        @Override
        public String getUUID() {
            return UUID.randomUUID().toString();
        }
    });

    @ClassRule
    public final static DropwizardClientRule learner = new DropwizardClientRule(PaxosLearnerImpl.newLearner("learner-log"));

    @ClassRule
    public final static DropwizardClientRule acceptor = new DropwizardClientRule(PaxosAcceptorImpl.newAcceptor("acceptor-log"));

    @Test
    public void testPing() {
        ObjectMapper mapper = new ObjectMapper();

        PingableLeader ping = Feign.builder()
                .decoder(new TextDelegateDecoder(new JacksonDecoder()))
                .encoder(new JacksonEncoder(mapper))
                .contract(new JAXRSContract())
                .target(PingableLeader.class, pingable.baseUri().toString());

        ping.getUUID();
        ping.ping();
    }

    @Test
    public void testLearn() throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        PaxosValue value = new PaxosValue("asdfasdfsa", 0, new byte[] {0, 1, 2});

        PaxosLearner learn = Feign.builder()
                .decoder(new TextDelegateDecoder(new JacksonDecoder()))
                .encoder(new JacksonEncoder(mapper))
                .contract(new JAXRSContract())
                .target(PaxosLearner.class, learner.baseUri().toString());

        learn.getGreatestLearnedValue();
        learn.getLearnedValuesSince(0);
        learn.learn(0, value);
        PaxosValue val = learn.getGreatestLearnedValue();
        learn.getLearnedValuesSince(0);
        learn.getLearnedValue(0);

    }

    @Test
    public void testAccept() throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        PaxosProposalId id = new PaxosProposalId(123123, UUID.randomUUID().toString());
        PaxosProposal paxosProposal = new PaxosProposal(id, new PaxosValue(id.getProposerUUID(), 0, new byte[] {0, 1, 2, 4, 1}));


        PaxosAcceptor accept = Feign.builder()
                .decoder(new TextDelegateDecoder(new JacksonDecoder()))
                .encoder(new JacksonEncoder(mapper))
                .contract(new JAXRSContract())
                .target(PaxosAcceptor.class, acceptor.baseUri().toString());

        accept.accept(0, paxosProposal);
        accept.getLatestSequencePreparedOrAccepted();
        accept.prepare(0, id);
        accept.prepare(1, id);
    }

}
