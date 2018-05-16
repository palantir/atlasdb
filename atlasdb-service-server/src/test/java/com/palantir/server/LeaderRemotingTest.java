/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
import java.util.Optional;
import java.util.UUID;

import org.junit.ClassRule;
import org.junit.Test;

import com.palantir.atlasdb.http.AtlasDbFeignTargetFactory;
import com.palantir.atlasdb.http.UserAgents;
import com.palantir.leader.PingableLeader;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosAcceptorImpl;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerImpl;
import com.palantir.paxos.PaxosProposal;
import com.palantir.paxos.PaxosProposalId;
import com.palantir.paxos.PaxosValue;

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
        PingableLeader ping = AtlasDbFeignTargetFactory.createProxy(
                Optional.empty(),
                Optional::empty,
                pingable.baseUri().toString(),
                PingableLeader.class,
                UserAgents.DEFAULT_USER_AGENT);

        ping.getUUID();
        ping.ping();
    }

    @Test
    public void testLearn() throws IOException {
        PaxosValue value = new PaxosValue("asdfasdfsa", 0, new byte[] {0, 1, 2});

        PaxosLearner learn = AtlasDbFeignTargetFactory.createProxy(
                Optional.empty(),
                Optional::empty,
                learner.baseUri().toString(),
                PaxosLearner.class,
                UserAgents.DEFAULT_USER_AGENT);

        learn.getGreatestLearnedValue();
        learn.getLearnedValuesSince(0);
        learn.learn(0, value);
        PaxosValue val = learn.getGreatestLearnedValue();
        learn.getLearnedValuesSince(0);
        learn.getLearnedValue(0);

    }

    @Test
    public void testAccept() throws IOException {
        PaxosProposalId id = new PaxosProposalId(123123, UUID.randomUUID().toString());
        PaxosProposal paxosProposal = new PaxosProposal(id, new PaxosValue(id.getProposerUUID(), 0, new byte[] {0, 1, 2, 4, 1}));

        PaxosAcceptor accept = AtlasDbFeignTargetFactory.createProxy(
                Optional.empty(),
                Optional::empty,
                acceptor.baseUri().toString(),
                PaxosAcceptor.class,
                UserAgents.DEFAULT_USER_AGENT);

        accept.accept(0, paxosProposal);
        accept.getLatestSequencePreparedOrAccepted();
        accept.prepare(0, id);
        accept.prepare(1, id);
    }

}
