/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.timelock.paxos;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.net.ssl.SSLSocketFactory;

import com.codahale.metrics.InstrumentedExecutorService;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.http.BlockingTimeoutExceptionMapper;
import com.palantir.atlasdb.http.NotCurrentLeaderExceptionMapper;
import com.palantir.atlasdb.http.UserAgents;
import com.palantir.atlasdb.timelock.TimeLockResource;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.TooManyRequestsExceptionMapper;
import com.palantir.atlasdb.timelock.paxos.CoordinationPaxosResource;
import com.palantir.atlasdb.timelock.paxos.ManagedTimestampService;
import com.palantir.atlasdb.timelock.paxos.PaxosResource;
import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.lock.RemoteLockService;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosAcceptorImpl;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerImpl;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosProposerImpl;
import com.palantir.remoting2.config.ssl.SslSocketFactories;
import com.palantir.timelock.Observables;
import com.palantir.timelock.TimeLockAgent;
import com.palantir.timelock.config.ImmutablePaxosRuntimeConfiguration;
import com.palantir.timelock.config.PaxosInstallConfiguration;
import com.palantir.timelock.config.TimeLockDeprecatedConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;
import com.palantir.timelock.coordination.CoordinationService;
import com.palantir.timelock.coordination.DrainService;
import com.palantir.timelock.coordination.DrainServiceImpl;
import com.palantir.timelock.coordination.HostTransition;
import com.palantir.timelock.coordination.ImmutableHostTransition;
import com.palantir.timelock.coordination.PaxosCoordinationService;
import com.palantir.timelock.partition.Assignment;
import com.palantir.timelock.partition.GreedyTimeLockPartitioner;
import com.palantir.timelock.partition.PartitionService;
import com.palantir.timelock.partition.PaxosPartitionService;
import com.palantir.timelock.partition.TimeLockPartitioner;

import io.reactivex.Observable;

public class PaxosAgent extends TimeLockAgent {
    private final PaxosInstallConfiguration paxosInstall;
    private final String localServer;
    private final Observable<Assignment> assignment;

    private final PaxosResource paxosResource;
    private final PaxosLeadershipCreator leadershipCreator;
    private final NamespacedPaxosLeadershipCreator namespacedPaxosLeadershipCreator;
    private final LockCreator lockCreator;
    private final PaxosTimestampCreator timestampCreator;
    private final TimeLockServicesCreator timelockCreator;

    private DrainServiceImpl drainService;
    private PartitionService partitionService;

    private PaxosAgent(TimeLockInstallConfiguration install,
            Observable<TimeLockRuntimeConfiguration> runtime,
            Observable<Assignment> assignment,
            TimeLockDeprecatedConfiguration deprecated,
            Consumer<Object> registrar) {
        super(install, runtime, deprecated, registrar);

        this.paxosInstall = install.algorithm();
        this.localServer = install.cluster().localServer();
        this.assignment = assignment;

        this.paxosResource = PaxosResource.create(paxosInstall.dataDirectory().toString());
        this.leadershipCreator = new PaxosLeadershipCreator(install, runtime, registrar);
        this.lockCreator = new LockCreator(runtime, deprecated);
        this.namespacedPaxosLeadershipCreator = new NamespacedPaxosLeadershipCreator(install, runtime, registrar);
        this.timestampCreator = new PaxosTimestampCreator(paxosResource,
                PaxosRemotingUtils.getRemoteServerPaths(install),
                PaxosRemotingUtils.getSslConfigurationOptional(install).map(SslSocketFactories::createSslSocketFactory),
                runtime.map(x -> x.algorithm().orElse(ImmutablePaxosRuntimeConfiguration.builder().build())));
        this.timelockCreator = install.asyncLock().useAsyncLockService()
                ? new AsyncTimeLockServicesCreator(namespacedPaxosLeadershipCreator, install.asyncLock())
                : new LegacyTimeLockServicesCreator(namespacedPaxosLeadershipCreator);
    }

    public static PaxosAgent create(TimeLockInstallConfiguration install,
            Observable<TimeLockRuntimeConfiguration> runtime,
            TimeLockDeprecatedConfiguration deprecated,
            Consumer<Object> registrar) {
        Observable<Assignment> assignment = runtime.map(runtimeConfig -> {
            TimeLockPartitioner partitioner = runtimeConfig.partitioner().createPartitioner();
            return partitioner.partition(
                    ImmutableList.copyOf(runtimeConfig.clients()),
                    install.cluster().cluster().uris(),
                    0L);
        });
        return new PaxosAgent(install,
                runtime,
                assignment,
                deprecated,
                registrar);
    }

    // No runtime configuration at the moment.
    private void registerPaxosResource() {
        namespacedPaxosLeadershipCreator.registerLeaderLocator();
        registrar.accept(paxosResource);
    }

    private void registerPaxosExceptionMappers() {
        registrar.accept(new BlockingTimeoutExceptionMapper());
        registrar.accept(new NotCurrentLeaderExceptionMapper());
        registrar.accept(new TooManyRequestsExceptionMapper());
    }

    @Override
    public void createAndRegisterResources() {
        leadershipCreator.registerLeaderElectionService();
        registerPaxosResource();
        registerPaxosExceptionMappers();

        Set<String> clients =
                Observables.blockingMostRecent(assignment.map(assign -> assign.getClientsForHost(localServer))).get();

        Map<String, TimeLockServices> clientToServices = Maps.newHashMap();
        drainService = new DrainServiceImpl(clientToServices, (unused, unused2) -> {});
        createAndRegisterPartitionService();
        Map<String, TimeLockServices> actualClientToServices =
                clients.stream().collect(Collectors.toMap(
                        Function.identity(),
                        this::createInvalidatingTimeLockServices));
        clientToServices.putAll(actualClientToServices);
        registrar.accept(drainService);
        registrar.accept(new TimeLockResource(clientToServices));
    }

    private void createAndRegisterPartitionService() {
        Set<String> remoteServers = PaxosRemotingUtils.getRemoteServerPaths(install);
        Set<String> clients =
                Observables.blockingMostRecent(assignment.map(assign -> assign.getClientsForHost(localServer))).get();

        // TODO (jkong): Don't hard-code
        PaxosAcceptor ourAcceptor = AtlasDbMetrics.instrument(
                PaxosAcceptor.class,
                PaxosAcceptorImpl.newAcceptor("var/data/partition/acceptor"));
        PaxosLearner ourLearner = AtlasDbMetrics.instrument(
                PaxosLearner.class,
                PaxosLearnerImpl.newLearner("var/data/partition/learner"));

        Optional<SSLSocketFactory> sslSocketFactory = PaxosRemotingUtils.getSslConfigurationOptional(install).map(
                SslSocketFactories::createSslSocketFactory);

        List<PaxosLearner> learners = createProxyAndLocalList(
                ourLearner,
                getNamespacedUris(remoteServers, PaxosTimeLockConstants.INTERNAL_NAMESPACE, "coordination"),
                sslSocketFactory, PaxosLearner.class, "coordinator-service");
        List<PaxosAcceptor> acceptors = createProxyAndLocalList(
                ourAcceptor,
                getNamespacedUris(remoteServers, PaxosTimeLockConstants.INTERNAL_NAMESPACE, "coordination"),
                sslSocketFactory,
                PaxosAcceptor.class,
                "coordinator-service");

        InstrumentedExecutorService proposerExecutorService = new InstrumentedExecutorService(
                Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                        .setNameFormat("atlas-coordination-proposer-%d")
                        .setDaemon(true)
                        .build()),
                AtlasDbMetrics.getMetricRegistry(),
                MetricRegistry.name(PaxosProposer.class, "executor"));

        PaxosProposer proposer = PaxosProposerImpl.newProposer(
                ourLearner,
                acceptors,
                learners,
                acceptors.size(),
                UUID.randomUUID(),
                proposerExecutorService);

        CoordinationService coordinationService = new PaxosCoordinationService(
                proposer,
                ourAcceptor,
                ourLearner,
                acceptors,
                learners);
        registrar.accept(new CoordinationPaxosResource(ourAcceptor, ourLearner));

        PartitionService partitionService = new PaxosPartitionService(
                coordinationService,
                createDrainServicesMap(),
                new GreedyTimeLockPartitioner(2), // TODO
                ImmutableList.copyOf(clients),
                ImmutableList.copyOf(PaxosRemotingUtils.addProtocols(install,
                        ImmutableSet.copyOf(install.cluster().cluster().uris()))));
        registrar.accept(leadershipCreator.wrapInLeadershipProxy(
                () -> partitionService,
                PartitionService.class));
    }

    private Map<String, DrainService> createDrainServicesMap() {
        Map<String, DrainService> clientToServices = Maps.newHashMap();
        Optional<SSLSocketFactory> sslSocketFactory = PaxosRemotingUtils.getSslConfigurationOptional(install).map(
                SslSocketFactories::createSslSocketFactory);

        clientToServices.put(PaxosRemotingUtils.addProtocol(install, localServer), drainService);
        PaxosRemotingUtils.getRemoteServerPaths(install).forEach(
                path -> clientToServices.put(path, AtlasDbHttpClients.createProxy(sslSocketFactory, path,
                        DrainService.class)));
        return clientToServices;
    }

    @Override
    protected TimeLockServices createInvalidatingTimeLockServices(String client) {
        Set<String> hostsForClient = PaxosRemotingUtils.addProtocols(install, Observables.blockingMostRecent(
                assignment.map(assign -> assign.getHostsForClient(client))).get());

        namespacedPaxosLeadershipCreator.registerLeaderElectionServiceForClient(client,
                ImmutableHostTransition.of(ImmutableSet.of(), hostsForClient));
        Supplier<ManagedTimestampService> rawTimestampServiceSupplier =
                timestampCreator.createPaxosBackedTimestampService(client,
                        namespacedPaxosLeadershipCreator.getNamespacedLeadershipResource(),
                        Sets.difference(hostsForClient, ImmutableSet.of(
                                PaxosRemotingUtils.addProtocol(install, localServer))));
        Supplier<RemoteLockService> rawLockServiceSupplier = lockCreator::createThreadPoolingLockService;

        Supplier<TimeLockServices> services =
                () -> timelockCreator.createTimeLockServices(client, rawTimestampServiceSupplier,
                        rawLockServiceSupplier);
        TimeLockServices actual = services.get();
        drainService.register(client, hosts -> createServicesForClient(client, hosts));
        return actual;
    }

    private TimeLockServices createServicesForClient(String client, HostTransition hosts) {
        namespacedPaxosLeadershipCreator.registerLeaderElectionServiceForClient(client, hosts);
        Supplier<ManagedTimestampService> rawTimestampServiceSupplier =
                timestampCreator.createPaxosBackedTimestampService(client,
                        namespacedPaxosLeadershipCreator.getNamespacedLeadershipResource(),
                        Sets.difference(hosts.newHostSet(), ImmutableSet.of(
                                PaxosRemotingUtils.addProtocol(install, localServer))),
                        hosts.oldHostSet());
        Supplier<RemoteLockService> rawLockServiceSupplier = lockCreator::createThreadPoolingLockService;

        Supplier<TimeLockServices> services =
                () -> timelockCreator.createTimeLockServices(client, rawTimestampServiceSupplier,
                        rawLockServiceSupplier);
        return services.get();
    }

    public static <T> List<T> createProxyAndLocalList(
            T localObject,
            Set<String> remoteUris,
            Optional<SSLSocketFactory> sslSocketFactory,
            Class<T> clazz) {
        return createProxyAndLocalList(localObject, remoteUris, sslSocketFactory, clazz, UserAgents.DEFAULT_USER_AGENT);
    }

    public static <T> List<T> createProxyAndLocalList(
            T localObject,
            Set<String> remoteUris,
            Optional<SSLSocketFactory> sslSocketFactory,
            Class<T> clazz,
            String userAgent) {
        return ImmutableList.copyOf(Iterables.concat(
                AtlasDbHttpClients.createProxies(sslSocketFactory, remoteUris, clazz, userAgent),
                ImmutableList.of(localObject)));
    }

    private static Set<String> getNamespacedUris(Set<String> addresses, String... suffixes) {
        String joinedSuffix = String.join("/", suffixes);
        return addresses.stream()
                .map(address -> String.join("/", address, joinedSuffix))
                .collect(Collectors.toSet());
    }
}
