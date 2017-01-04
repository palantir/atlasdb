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
import java.util.Set;
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
import com.palantir.atlasdb.http.NotCurrentLeaderExceptionMapper;
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
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosProposerImpl;
import com.palantir.remoting.ssl.SslConfiguration;
import com.palantir.remoting.ssl.SslSocketFactories;
import com.palantir.timestamp.PersistentTimestampService;
import com.palantir.timestamp.TimestampService;

import io.atomix.catalyst.transport.Address;
import io.dropwizard.setup.Environment;

public class PaxosServerImplementation implements ServerImplementation {
    private Set<String> remoteServers;
    private Optional<SSLSocketFactory> optionalSecurity = Optional.absent();

    private LeaderElectionService leaderElectionService;
    private PaxosResource paxosResource;

    private final Environment environment;

    public PaxosServerImplementation(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void onStart(TimeLockServerConfiguration configuration) {
        String leaderNamespace = "__leader";
        paxosResource = PaxosResource.create();
        paxosResource.addClient(leaderNamespace);

        if (configuration.atomix().security().isPresent()) {
            optionalSecurity = constructOptionalSslSocketFactory(configuration);
        }

        remoteServers = getRemoteAddresses(configuration);
        // Construct the intermediate stuff
        List<PaxosLearner> learners = getNamespacedProxies(
                getRemoteAddresses(configuration),
                "/" + leaderNamespace,
                optionalSecurity,
                PaxosLearner.class);
        learners.add(paxosResource.getPaxosLearner(leaderNamespace));

        List<PaxosAcceptor> acceptors = getNamespacedProxies(
                getRemoteAddresses(configuration),
                "/" + leaderNamespace,
                optionalSecurity,
                PaxosAcceptor.class);
        acceptors.add(paxosResource.getPaxosAcceptor(leaderNamespace));

        Map<PingableLeader, HostAndPort> otherLeaders = Leaders.generatePingables(
                Sets.difference(configuration.cluster().servers().stream().map(x -> "http://" + x.host() + ":" + x.port())
                                .collect(Collectors.toSet()),
                        ImmutableSet.of("http://" + configuration.cluster().localServer().host() + ":" + configuration.cluster().localServer().port())),
                optionalSecurity);

        ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                .setNameFormat("atlas-leaders-%d")
                .setDaemon(true)
                .build());

        PaxosProposer proposer = PaxosProposerImpl.newProposer(
                paxosResource.getPaxosLearner(leaderNamespace),
                ImmutableList.copyOf(acceptors),
                ImmutableList.copyOf(learners),
                configuration.cluster().servers().size() / 2 + 1,
                executor);

        // Build and store a gatekeeping leader election service
        PaxosLeaderElectionService leader = new PaxosLeaderElectionService(
                proposer,
                paxosResource.getPaxosLearner(leaderNamespace),
                otherLeaders,
                ImmutableList.copyOf(acceptors),
                ImmutableList.copyOf(learners),
                executor,
                1000L, // Ping rate ms
                5000L, // Wait before proposing
                2000L); // Leader ping response wait
        leaderElectionService = leader;

        environment.jersey().register(paxosResource);
        environment.jersey().register(leaderElectionService);
        environment.jersey().register(new NotCurrentLeaderExceptionMapper());
    }

    private static Optional<SSLSocketFactory> constructOptionalSslSocketFactory(
            TimeLockServerConfiguration configuration) {
        return Optional.of(SslSocketFactories.createSslSocketFactory(
                SslConfiguration.of(
                        configuration.atomix().security().get().trustStorePath(),
                        configuration.atomix().security().get().keyStorePath().orNull(),
                        configuration.atomix().security().get().keyStorePassword().orNull())));
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
        paxosResource.addClient(client);

        ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                .setNameFormat("atlas-consensus-%d")
                .setDaemon(true)
                .build());

        List<PaxosAcceptor> acceptors = getNamespacedProxies(remoteServers,
                "/" + client,
                optionalSecurity,
                PaxosAcceptor.class);
        acceptors.add(paxosResource.getPaxosAcceptor(client));
        List<PaxosLearner> learners = getNamespacedProxies(remoteServers,
                "/" + client,
                optionalSecurity,
                PaxosLearner.class);
        learners.add(paxosResource.getPaxosLearner(client));

        PaxosProposer proposer = PaxosProposerImpl.newProposer(
                paxosResource.getPaxosLearner(client),
                ImmutableList.copyOf(acceptors),
                ImmutableList.copyOf(learners),
                acceptors.size() / 2 + 1,
                executor);

        TimestampService timestampService = AwaitingLeadershipProxy.newProxyInstance(
                TimestampService.class,
                () -> PersistentTimestampService.create(
                        new PaxosTimestampBoundStore(
                                proposer,
                                paxosResource.getPaxosLearner(client),
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

    private static Set<String> getRemoteAddresses(TimeLockServerConfiguration configuration) {
        Set<Address> remoteAddresses = Sets.difference(
                configuration.cluster().servers(),
                ImmutableSet.of(configuration.cluster().localServer()));
        return remoteAddresses.stream()
                .map(x -> "http://" + x.host() + ":" + x.port())
                .collect(Collectors.toSet());
    }

    private static Set<String> getSuffixedUris(Set<String> addresses, String suffix) {
        return addresses.stream()
                .map(address -> address + suffix)
                .collect(Collectors.toSet());
    }

    private static <T> List<T> getNamespacedProxies(Set<String> addresses,
                                                   String namespace,
                                                   Optional<SSLSocketFactory> optionalSecurity,
                                                   Class<T> clazz) {
        Set<String> endpointUris = getSuffixedUris(addresses, namespace);
        return AtlasDbHttpClients.createProxies(optionalSecurity, endpointUris, clazz);
    }

}
