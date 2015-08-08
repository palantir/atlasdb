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

import java.util.concurrent.ExecutorService;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.client.TextDelegateDecoder;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.leader.PaxosLeaderElectionService;
import com.palantir.leader.PingableLeader;
import com.palantir.leader.proxy.AwaitingLeadershipProxy;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosAcceptorImpl;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerImpl;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosProposerImpl;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.RateLimitedTimestampService;
import com.palantir.timestamp.TimestampService;
import com.palantir.timestamp.server.config.TimestampServerConfiguration;

import feign.Feign;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.jaxrs.JAXRSContract;
import io.dropwizard.Application;
import io.dropwizard.setup.Environment;

public class TimestampServer extends Application<TimestampServerConfiguration> {

    public static void main(String[] args) throws Exception {
        new TimestampServer().run(args);
    }

    private final ExecutorService executor = PTExecutors.newCachedThreadPool();

    @Override
    public void run(TimestampServerConfiguration configuration, Environment environment) throws Exception {
    	String extServer = configuration.servers.iterator().next();
    	PaxosLearner learner = PaxosLearnerImpl.newLearner(configuration.learnerLogDir);
    	PaxosAcceptor acceptor = PaxosAcceptorImpl.newAcceptor(configuration.acceptorLogDir);
        environment.jersey().register(acceptor);
        environment.jersey().register(learner);
    	ObjectMapper mapper = new ObjectMapper();
        PingableLeader extLeader = Feign.builder()
                .decoder(new TextDelegateDecoder(new JacksonDecoder()))
                .encoder(new JacksonEncoder(mapper))
                .contract(new JAXRSContract())
                .target(PingableLeader.class, extServer);
        PaxosLearner extLearner = Feign.builder()
                .decoder(new TextDelegateDecoder(new JacksonDecoder()))
                .encoder(new JacksonEncoder(mapper))
                .contract(new JAXRSContract())
                .target(PaxosLearner.class, extServer);
        PaxosAcceptor extAcceptor = Feign.builder()
                .decoder(new TextDelegateDecoder(new JacksonDecoder()))
                .encoder(new JacksonEncoder(mapper))
                .contract(new JAXRSContract())
                .target(PaxosAcceptor.class, extServer);

        PaxosProposer proposer = PaxosProposerImpl.newProposer(
        		learner,
        		ImmutableList.<PaxosAcceptor>of(acceptor),
        		ImmutableList.<PaxosLearner>of(learner),
        		configuration.quorumSize,
        		executor);
        PaxosLeaderElectionService leader = new PaxosLeaderElectionService(
                proposer,
                learner,
                ImmutableList.of(extLeader),
                ImmutableList.of(extAcceptor),
                ImmutableList.of(extLearner),
                executor,
                0,
                0,
                0);
        environment.jersey().register(leader);

        TimestampService timestamp = AwaitingLeadershipProxy.newProxyInstance(TimestampService.class, new Supplier<TimestampService>() {
            @Override
            public TimestampService get() {
                // TODO: actually use persistent TS
                return new RateLimitedTimestampService(new InMemoryTimestampService(), 0L);
            }
        }, leader);
        environment.jersey().register(timestamp);

        RemoteLockService lock = AwaitingLeadershipProxy.newProxyInstance(RemoteLockService.class, new Supplier<RemoteLockService>() {
            @Override
            public RemoteLockService get() {
                return LockServiceImpl.create();
            }
        }, leader);
        environment.jersey().register(lock);
    }
}
