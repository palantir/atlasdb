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
package com.palantir.atlasdb.server;

import java.util.List;
import java.util.concurrent.ExecutorService;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.client.TextDelegateDecoder;
import com.palantir.atlasdb.impl.AtlasDbServiceImpl;
import com.palantir.atlasdb.impl.TableMetadataCache;
import com.palantir.atlasdb.jackson.AtlasJacksonModule;
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
import com.palantir.timestamp.TimestampService;
import com.palantir.timestamp.server.config.AtlasDbServerConfiguration;
import com.palantir.timestamp.server.config.AtlasDbServerState;

import feign.Feign;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.jaxrs.JAXRSContract;
import io.dropwizard.Application;
import io.dropwizard.setup.Environment;
import jersey.repackaged.com.google.common.collect.Lists;

public class AtlasDbServer extends Application<AtlasDbServerConfiguration> {

    public static void main(String[] args) throws Exception {
        new AtlasDbServer().run(args);
    }

    private final ExecutorService executor = PTExecutors.newCachedThreadPool();

    private static <T> List<T> getRemoteServices(List<String> uris, Class<T> iFace) {
    	ObjectMapper mapper = new ObjectMapper();
        List<T> ret = Lists.newArrayList();
        for (String uri : uris) {
            T service = Feign.builder()
                    .decoder(new TextDelegateDecoder(new JacksonDecoder(mapper)))
                    .encoder(new JacksonEncoder(mapper))
                    .contract(new JAXRSContract())
                    .target(iFace, uri);
            ret.add(service);
        }
        return ret;
    }

    public static ObjectMapper getObjectMapper(TableMetadataCache cache) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new AtlasJacksonModule(cache).createModule());
        return mapper;
    }

    @Override
    public void run(AtlasDbServerConfiguration configuration, Environment environment) throws Exception {
    	PaxosLearner learner = PaxosLearnerImpl.newLearner(configuration.leader.learnerLogDir);
    	PaxosAcceptor acceptor = PaxosAcceptorImpl.newAcceptor(configuration.leader.acceptorLogDir);
        environment.jersey().register(acceptor);
        environment.jersey().register(learner);

        int localIndex = configuration.leader.leaders.indexOf(configuration.leader.localServer);
        Preconditions.checkArgument(localIndex != -1, "localServer must be in the list of leaders");

        List<PaxosLearner> learners = getRemoteServices(configuration.leader.leaders, PaxosLearner.class);
        learners.set(localIndex, learner);
        List<PaxosAcceptor> acceptors = getRemoteServices(configuration.leader.leaders, PaxosAcceptor.class);
        acceptors.set(localIndex, acceptor);

        List<PingableLeader> otherLeaders = getRemoteServices(configuration.leader.leaders, PingableLeader.class);
        otherLeaders.remove(localIndex);

        PaxosProposer proposer = PaxosProposerImpl.newProposer(
        		learner,
        		acceptors,
        		learners,
        		configuration.leader.quorumSize,
        		executor);
        PaxosLeaderElectionService leader = new PaxosLeaderElectionService(
                proposer,
                learner,
                otherLeaders,
                acceptors,
                learners,
                executor,
                5000,
                1000,
                5000);
        environment.jersey().register(leader);
        environment.jersey().register(new NotCurrentLeaderExceptionMapper());

        environment.jersey().register(createLockService(leader));

        AtlasDbServerState factory = AtlasDbServerState.create(configuration);
        environment.jersey().register(AwaitingLeadershipProxy.newProxyInstance(TimestampService.class, factory.getTimestampSupplier(), leader));
        TableMetadataCache cache = new TableMetadataCache(factory.getKeyValueService());
        environment.jersey().register(new AtlasDbServiceImpl(factory.getKeyValueService(), factory.getTransactionManager(), cache));
        environment.getObjectMapper().registerModule(new AtlasJacksonModule(cache).createModule());
    }

    private RemoteLockService createLockService(PaxosLeaderElectionService leader) {
        return AwaitingLeadershipProxy.newProxyInstance(RemoteLockService.class, new Supplier<RemoteLockService>() {
            @Override
            public RemoteLockService get() {
                return LockServiceImpl.create();
            }
        }, leader);
    }
}
