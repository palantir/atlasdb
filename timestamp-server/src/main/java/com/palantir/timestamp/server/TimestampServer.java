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

import java.util.List;
import java.util.concurrent.ExecutorService;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.palantir.atlas.impl.AtlasServiceImpl;
import com.palantir.atlas.impl.TableMetadataCache;
import com.palantir.atlas.jackson.AtlasJacksonModule;
import com.palantir.atlasdb.client.FailoverFeignTarget;
import com.palantir.atlasdb.client.TextDelegateDecoder;
import com.palantir.atlasdb.table.description.Schema;
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
import com.palantir.timestamp.server.config.AtlasDbServerFactory;
import com.palantir.timestamp.server.config.CassandraAtlasServerFactory;
import com.palantir.timestamp.server.config.LevelDbAtlasServerFactory;
import com.palantir.timestamp.server.config.TimestampServerConfiguration;
import com.palantir.timestamp.server.config.TimestampServerConfiguration.ServerType;

import feign.Client;
import feign.Feign;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.jaxrs.JAXRSContract;
import io.dropwizard.Application;
import io.dropwizard.setup.Environment;
import jersey.repackaged.com.google.common.collect.Lists;

public class TimestampServer extends Application<TimestampServerConfiguration> {

    public static void main(String[] args) throws Exception {
        new TimestampServer().run(args);
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

    private static <T> T getServiceWithFailover(List<String> uris, Class<T> type) {
    	ObjectMapper mapper = new ObjectMapper();
    	FailoverFeignTarget<T> failoverFeignTarget = new FailoverFeignTarget<T>(uris, type);
    	Client client = failoverFeignTarget.wrapClient(new Client.Default(null, null));
        return Feign.builder()
                .decoder(new TextDelegateDecoder(new JacksonDecoder(mapper)))
                .encoder(new JacksonEncoder(mapper))
                .contract(new JAXRSContract())
                .client(client)
                .retryer(failoverFeignTarget)
                .target(failoverFeignTarget);
    }

    public static ObjectMapper getObjectMapper(TableMetadataCache cache) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new AtlasJacksonModule(cache).createModule());
        return mapper;
    }

    @Override
    public void run(TimestampServerConfiguration configuration, Environment environment) throws Exception {
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
                1000,
                1000,
                5000);
        environment.jersey().register(leader);
        environment.jersey().register(new NotCurrentLeaderExceptionMapper());

        environment.jersey().register(createLockService(leader));

        AtlasDbServerFactory factory = createFactory(configuration);
        environment.jersey().register(AwaitingLeadershipProxy.newProxyInstance(TimestampService.class, factory.getTimestampSupplier(), leader));
        TableMetadataCache cache = new TableMetadataCache(factory.getKeyValueService());
        environment.jersey().register(new AtlasServiceImpl(factory.getKeyValueService(), factory.getTransactionManager(), cache));
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

    private AtlasDbServerFactory createFactory(final TimestampServerConfiguration config) {
        RemoteLockService leadingLock = getServiceWithFailover(config.lockClient.servers, RemoteLockService.class);
        TimestampService leadingTs = getServiceWithFailover(config.timestampClient.servers, TimestampService.class);

        if (config.serverType == ServerType.LEVELDB) {
            Preconditions.checkArgument(config.leader.leaders.size() == 1, "only one server allowed for LevelDB");
            return LevelDbAtlasServerFactory.create(config.levelDbDir, new Schema(), leadingTs, leadingLock);
        } else {
            return CassandraAtlasServerFactory.create(config.cassandra, new Schema(), leadingTs, leadingLock);
        }
    }
}
