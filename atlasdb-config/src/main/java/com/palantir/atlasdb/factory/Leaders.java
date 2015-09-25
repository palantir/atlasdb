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
package com.palantir.atlasdb.factory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.net.ssl.SSLSocketFactory;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.factory.TransactionManagers.Environment;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.http.NotCurrentLeaderExceptionMapper;
import com.palantir.leader.LeaderElectionService;
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

public class Leaders {

    /**
     * Creates a LeaderElectionService using the supplied configuration, registers appropriate endpoints
     * for that service, then uses that service to create and registers leader-aware instances of RemoteLockService 
     * and TimestampService.
     */
    public static TimestampService createLockAndTimestampServices(
            LeaderConfig config,
            Optional<SSLSocketFactory> sslSocketFactory,
            final TimestampService timestampService,
            Environment env) {
        LeaderElectionService leader = Leaders.create(sslSocketFactory, env, config);
        
        Supplier<RemoteLockService> lock = new Supplier<RemoteLockService>() {
            @Override
            public RemoteLockService get() {
                return LockServiceImpl.create();
            }
        };
        
        Supplier<TimestampService> timestamp = new Supplier<TimestampService>() {
            @Override
            public TimestampService get() {
                return timestampService;
            }
        };
        
        env.register(AwaitingLeadershipProxy.newProxyInstance(RemoteLockService.class, lock, leader));
        TimestampService localTs = AwaitingLeadershipProxy.newProxyInstance(TimestampService.class, timestamp, leader);
        env.register(localTs);
        return localTs;
    }
    
    private static LeaderElectionService create(
            Optional<SSLSocketFactory> sslSocketFactory,
            Environment env,
            LeaderConfig config) {

        PaxosAcceptor ourAcceptor = PaxosAcceptorImpl.newAcceptor(config.getAcceptorLogDir().getPath());
        PaxosLearner ourLearner = PaxosLearnerImpl.newLearner(config.getLearnerLogDir().getPath());

        Set<String> remoteLeaderUris = Sets.newHashSet(config.getLeaders());
        remoteLeaderUris.remove(config.getLocalServer());

        List<PaxosLearner> learners =
                AtlasDbHttpClients.createProxies(sslSocketFactory, remoteLeaderUris, PaxosLearner.class);
        learners.add(ourLearner);

        List<PaxosAcceptor> acceptors =
                AtlasDbHttpClients.createProxies(sslSocketFactory, remoteLeaderUris, PaxosAcceptor.class);
        acceptors.add(ourAcceptor);

        List<PingableLeader> otherLeaders =
                AtlasDbHttpClients.createProxies(sslSocketFactory, remoteLeaderUris, PingableLeader.class);

        ExecutorService executor = Executors.newCachedThreadPool();

        PaxosProposer proposer = PaxosProposerImpl.newProposer(
                ourLearner,
                acceptors,
                learners,
                config.getQuorumSize(),
                executor);

        PaxosLeaderElectionService leader = new PaxosLeaderElectionService(
                proposer,
                ourLearner,
                otherLeaders,
                acceptors,
                learners,
                executor,
                config.getPingRateMs(),
                config.getRandomWaitBeforeProposingLeadershipMs(),
                config.getLeaderPingResponseWaitMs());

        env.register(ourAcceptor);
        env.register(ourLearner);

        env.register(leader);
        env.register(new NotCurrentLeaderExceptionMapper());

        return leader;
    }
    
}
