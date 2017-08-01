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

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.http.BlockingTimeoutExceptionMapper;
import com.palantir.atlasdb.http.NotCurrentLeaderExceptionMapper;
import com.palantir.atlasdb.timelock.TimeLockResource;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.TooManyRequestsExceptionMapper;
import com.palantir.atlasdb.timelock.paxos.ManagedTimestampService;
import com.palantir.atlasdb.timelock.paxos.PaxosResource;
import com.palantir.lock.RemoteLockService;
import com.palantir.remoting2.config.ssl.SslSocketFactories;
import com.palantir.timelock.Observables;
import com.palantir.timelock.TimeLockAgent;
import com.palantir.timelock.config.ImmutablePaxosRuntimeConfiguration;
import com.palantir.timelock.config.PaxosInstallConfiguration;
import com.palantir.timelock.config.TimeLockDeprecatedConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;
import com.palantir.timelock.coordination.DrainService;
import com.palantir.timelock.coordination.DrainServiceImpl;
import com.palantir.timelock.partition.Assignment;
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

    private DrainService drainService;

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
        drainService = new DrainServiceImpl(clientToServices,
                client -> {
            Set<String> hostsForClient = PaxosRemotingUtils.addProtocols(install, Observables.blockingMostRecent(
                            assignment.map(assign -> assign.getHostsForClient(client))).get());
            namespacedPaxosLeadershipCreator.registerLeaderElectionServiceForClient(client, hostsForClient);
                });
        Map<String, TimeLockServices> actualClientToServices =
                clients.stream().collect(Collectors.toMap(
                        Function.identity(),
                        this::createInvalidatingTimeLockServices));
        clientToServices.putAll(actualClientToServices);
        registrar.accept(drainService);
        registrar.accept(new TimeLockResource(clientToServices));
    }

    @Override
    protected TimeLockServices createInvalidatingTimeLockServices(String client) {
        Set<String> hostsForClient = PaxosRemotingUtils.addProtocols(install, Observables.blockingMostRecent(
                assignment.map(assign -> assign.getHostsForClient(client))).get());

        namespacedPaxosLeadershipCreator.registerLeaderElectionServiceForClient(client, hostsForClient);
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
        drainService.register(client, services);
        return actual;
    }
}
