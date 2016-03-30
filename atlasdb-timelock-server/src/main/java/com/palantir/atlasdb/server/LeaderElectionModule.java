/**
 * Copyright 2016 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.server;

import static java.util.function.Predicate.isEqual;
import static java.util.stream.Collectors.toSet;

import static com.palantir.common.concurrent.PTExecutors.newSingleThreadExecutor;
import static com.palantir.paxos.PaxosAcceptorImpl.newAcceptor;
import static com.palantir.paxos.PaxosLearnerImpl.newLearner;
import static com.palantir.paxos.PaxosProposerImpl.newProposer;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

import javax.inject.Singleton;
import javax.net.ssl.SSLSocketFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.PaxosLeaderElectionService;
import com.palantir.leader.PingableLeader;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosProposer;

import dagger.Module;
import dagger.Provides;

@Module
public class LeaderElectionModule {
    @Provides
    @Singleton
    public LeaderElectionService leaderElectionService(
            LeaderConfig config,
            @Local PaxosProposer proposer,
            @Local PaxosLearner learner,
            @All List<PaxosAcceptor> acceptors,
            @All List<PaxosLearner> learners,
            @Remote Map<PingableLeader, HostAndPort> potentialLeaders) {

        return new PaxosLeaderElectionService(
                proposer,
                learner,
                potentialLeaders,
                acceptors,
                learners,
                Executors.newSingleThreadExecutor(),
                config.pingRateMs(),
                config.randomWaitBeforeProposingLeadershipMs(),
                config.leaderPingResponseWaitMs());
    }

    @Provides
    @Singleton
    @All
    public List<PaxosAcceptor> provideAllPaxosAcceptors(@Remote Set<String> remoteLeaders, @Local PaxosAcceptor localAcceptor, Optional<SSLSocketFactory> sslSocketFactory) {
        List<PaxosAcceptor> remoteAcceptors = AtlasDbHttpClients.createProxies(sslSocketFactory, remoteLeaders, PaxosAcceptor.class);

        return ImmutableList.<PaxosAcceptor>builder()
                .addAll(remoteAcceptors)
                .add(localAcceptor)
                .build();
    }

    @Provides
    @Singleton
    @All
    public List<PaxosLearner> provideAllPaxosLearners(@Remote Set<String> remoteLeaders, @Local PaxosLearner localLearner, Optional<SSLSocketFactory> sslSocketFactory) {
        List<PaxosLearner> remoteLearners = AtlasDbHttpClients.createProxies(sslSocketFactory, remoteLeaders, PaxosLearner.class);

        return ImmutableList.<PaxosLearner>builder()
                .addAll(remoteLearners)
                .add(localLearner)
                .build();
    }

    @Provides
    @Singleton
    @Local
    public PaxosAcceptor providePaxosAcceptor(LeaderConfig config) {
        return newAcceptor(config.acceptorLogDir().getPath());
    }

    @Provides
    @Singleton
    @Local
    public PaxosLearner providePaxosLearner(LeaderConfig config) {
        return newLearner(config.learnerLogDir().getPath());
    }

    @Provides
    @Singleton
    @Local
    public PaxosProposer providePaxosProposer(
            @Local PaxosLearner localLearner,
            LeaderConfig config,
            @All List<PaxosAcceptor> allAcceptors,
            @All List<PaxosLearner> allLearners) {
        return newProposer(
                localLearner,
                allAcceptors,
                allLearners,
                config.quorumSize(),
                newSingleThreadExecutor());
    }

    @Provides
    @Singleton
    @Remote
    public Set<String> provideRemoteLeaders(LeaderConfig config) {
        return config.leaders().stream()
                .filter(isEqual(config.localServer()).negate())
                .collect(toSet());
    }


    @Provides
    @Singleton
    @Remote
    public Map<PingableLeader, HostAndPort> providePotentialLeaders(@Remote Set<String> remoteLeaders, Optional<SSLSocketFactory> sslSocketFactory) {
        /* The interface used as a key here may be a proxy, which may have strange .equals() behavior.
         * This is circumvented by using an IdentityHashMap which will just use native == for equality.
         */
        Map<PingableLeader, HostAndPort> pingables = new IdentityHashMap<PingableLeader, HostAndPort>();

        for (String endpoint : remoteLeaders) {
            PingableLeader remoteInterface = AtlasDbHttpClients.createProxy(sslSocketFactory, endpoint, PingableLeader.class);
            HostAndPort hostAndPort = HostAndPort.fromString(endpoint);
            pingables.put(remoteInterface, hostAndPort);
        }
        return pingables;
    }
}
