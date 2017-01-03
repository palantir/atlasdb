/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.timelock.paxos;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.net.ssl.SSLSocketFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.factory.Leaders;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.timelock.ServerImplementation;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.PaxosLeaderElectionService;
import com.palantir.leader.PingableLeader;
import com.palantir.leader.proxy.AwaitingLeadershipProxy;
import com.palantir.lock.LockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosAcceptorImpl;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerImpl;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosProposerImpl;
import com.palantir.remoting.ssl.SslConfiguration;
import com.palantir.remoting.ssl.SslSocketFactories;
import com.palantir.timestamp.PersistentTimestampService;
import com.palantir.timestamp.TimestampService;

import io.atomix.catalyst.transport.Address;

public class PaxosServerImplementation implements ServerImplementation {
    private Optional<SSLSocketFactory> sslSocketFactory = Optional.absent();

    private List<PaxosLearner> learners;
    private List<PaxosAcceptor> acceptors;

    private LeaderElectionService leaderElectionService;

    @Override
    public void onStart(TimeLockServerConfiguration configuration) {
        PaxosLearner ourLearner = PaxosLearnerImpl.newLearner("paxos");
        PaxosAcceptor ourAcceptor = PaxosAcceptorImpl.newAcceptor("paxossss");

        if (configuration.atomix().security().isPresent()) {
            sslSocketFactory = Optional.of(SslSocketFactories.createSslSocketFactory(
                    SslConfiguration.of(
                            configuration.atomix().security().get().trustStorePath(),
                            configuration.atomix().security().get().keyStorePath().orNull(),
                            configuration.atomix().security().get().keyStorePassword().orNull())));
        }

        // Construct the intermediate stuff
        learners = configuration.cluster()
                .servers()
                .stream()
                .filter(address -> !address.equals(configuration.cluster().localServer()))
                .map(address -> address.toString() + "/leader")
                .map(address -> AtlasDbHttpClients.createProxy(
                        sslSocketFactory,
                        address,
                        PaxosLearner.class))
                .collect(Collectors.toList());
        learners.add(ourLearner);

        acceptors = configuration.cluster()
                .servers()
                .stream()
                .filter(address -> !address.equals(configuration.cluster().localServer()))
                .map(address -> address.toString() + "/leader")
                .map(address -> AtlasDbHttpClients.createProxy(
                        sslSocketFactory,
                        address,
                        PaxosAcceptor.class))
                .collect(Collectors.toList());
        acceptors.add(ourAcceptor);

        Map<PingableLeader, HostAndPort> otherLeaders = Leaders.generatePingables(
                Sets.difference(configuration.cluster().servers().stream().map(Address::toString).collect(Collectors.toSet()),
                        ImmutableSet.of(configuration.cluster().localServer())),
                sslSocketFactory);

        ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                .setNameFormat("atlas-leaders-%d")
                .setDaemon(true)
                .build());

        PaxosProposer proposer = PaxosProposerImpl.newProposer(
                ourLearner,
                ImmutableList.copyOf(acceptors),
                ImmutableList.copyOf(learners),
                configuration.cluster().servers().size() / 2 + 1,
                executor);

        // Build and store a gatekeeping leader election service
        PaxosLeaderElectionService leader = new PaxosLeaderElectionService(
                proposer,
                ourLearner,
                otherLeaders,
                ImmutableList.copyOf(acceptors),
                ImmutableList.copyOf(learners),
                executor,
                1000L, // Ping rate ms
                5000L, // Wait before proposing
                2000L); // Leader ping response wait
        leaderElectionService = leader;
    }

    @Override
    public void onStop() {
        // Nothing to do
    }

    @Override
    public void onFail() {
        // Nothing
    }

    @Override
    public TimeLockServices createInvalidatingTimeLockServices(String client) {
        // Establish a group of Paxos coordinators for THIS CLIENT that achieve consensus
        PaxosLearner ourLearner = PaxosLearnerImpl.newLearner("paxos/timestamp/" + client + "/learner");

        ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                .setNameFormat("atlas-consensus-%d")
                .setDaemon(true)
                .build());

        PaxosProposer proposer = PaxosProposerImpl.newProposer(
                ourLearner,
                ImmutableList.copyOf(acceptors),
                ImmutableList.copyOf(learners),
                acceptors.size() / 2 + 1,
                executor
        );

        TimestampService timestampService = AwaitingLeadershipProxy.newProxyInstance(
                TimestampService.class,
                () -> PersistentTimestampService.create(
                        new PaxosTimestampBoundStore(
                                proposer,
                                ourLearner,
                                ImmutableList.copyOf(acceptors),
                                ImmutableList.copyOf(learners)
                        )
                ),
                leaderElectionService);

        LockService lockService = AwaitingLeadershipProxy.newProxyInstance(
                LockService.class,
                LockServiceImpl::create,
                leaderElectionService);

        return TimeLockServices.create(timestampService, lockService);
    }
}
