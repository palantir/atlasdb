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

import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.http.BlockingTimeoutExceptionMapper;
import com.palantir.atlasdb.http.NotCurrentLeaderExceptionMapper;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.paxos.ManagedTimestampService;
import com.palantir.atlasdb.timelock.paxos.PaxosResource;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.lock.RemoteLockService;
import com.palantir.remoting2.config.ssl.SslConfiguration;
import com.palantir.remoting2.config.ssl.SslSocketFactories;
import com.palantir.timelock.TimeLockAgent;
import com.palantir.timelock.config.ClusterConfiguration;
import com.palantir.timelock.config.ImmutablePaxosRuntimeConfiguration;
import com.palantir.timelock.config.ImmutableTimeLockDeprecatedConfiguration;
import com.palantir.timelock.config.PaxosInstallConfiguration;
import com.palantir.timelock.config.PaxosRuntimeConfiguration;
import com.palantir.timelock.config.TimeLockDeprecatedConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;

import io.reactivex.Observable;

public class PaxosAgent extends TimeLockAgent {
    private final PaxosInstallConfiguration paxosInstall;

    private final PaxosResource paxosResource;
    private final PaxosLeadershipCreator leadershipAgent;
    private final LockCreator lockCreator;
    private final PaxosTimestampCreator timestampCreator;

    public PaxosAgent(TimeLockInstallConfiguration install,
            Observable<TimeLockRuntimeConfiguration> runtime,
            Consumer<Object> registrar) {
        this(install, runtime, ImmutableTimeLockDeprecatedConfiguration.builder().build(), registrar);
    }

    public PaxosAgent(TimeLockInstallConfiguration install,
            Observable<TimeLockRuntimeConfiguration> runtime,
            TimeLockDeprecatedConfiguration deprecated,
            Consumer<Object> registrar) {
        super(install, runtime, deprecated, registrar);
        Preconditions.checkState(install.algorithm() instanceof PaxosInstallConfiguration,
                "Cannot initialize a Paxos agent with a non-Paxos algorithm configuration.");
        this.paxosInstall = ((PaxosInstallConfiguration) install.algorithm());

        this.paxosResource = PaxosResource.create(paxosInstall.dataDirectory().toString());
        this.leadershipAgent = new PaxosLeadershipCreator(install, runtime, registrar);
        this.lockCreator = new LockCreator(runtime, deprecated);
        this.timestampCreator = new PaxosTimestampCreator(paxosResource,
                getRemoteServerPaths(),
                install.cluster().cluster().security().map(SslSocketFactories::createSslSocketFactory),
                runtime.map(x -> x.algorithm().orElse(ImmutablePaxosRuntimeConfiguration.builder().build())));
    }

    @Override
    public void createAndRegisterResources() {
        registerPaxosResource();
        registerPaxosExceptionMappers();
        leadershipAgent.registerLeaderElectionService();

        // Finally, register the endpoints associated with the clients.
        super.createAndRegisterResources();
    }

    // No runtime configuration at the moment.
    private void registerPaxosResource() {
        registrar.accept(paxosResource);
    }

    private void registerPaxosExceptionMappers() {
        registrar.accept(new BlockingTimeoutExceptionMapper());
        registrar.accept(new NotCurrentLeaderExceptionMapper());
    }

    @Override
    public TimeLockServices createInvalidatingTimeLockServices(String client) {
        ManagedTimestampService timestampService = instrument(
                ManagedTimestampService.class,
                leadershipAgent.wrapInLeadershipProxy(
                        timestampCreator.createPaxosBackedTimestampService(client),
                        ManagedTimestampService.class),
                client);
        RemoteLockService lockService = instrument(
                RemoteLockService.class,
                leadershipAgent.wrapInLeadershipProxy(
                        lockCreator::createThreadPoolingLockService,
                        RemoteLockService.class),
                client);

        return TimeLockServices.create(timestampService, lockService, timestampService);
    }

    @Override
    protected boolean configurationFilter(TimeLockRuntimeConfiguration runtimeConfiguration) {
        // Note: Allow empty, in which case we use the default params
        return !runtimeConfiguration.algorithm().isPresent()
                || runtimeConfiguration.algorithm().get() instanceof PaxosRuntimeConfiguration;
    }

    private static <T> T instrument(Class<T> serviceClass, T service, String client) {
        return AtlasDbMetrics.instrument(serviceClass, service, MetricRegistry.name(serviceClass, client));
    }

    private ImmutableSet<String> getClusterAddresses() {
        return ImmutableSet.copyOf(getClusterConfiguration().cluster().uris());
    }

    private Set<String> getRemoteServerAddresses() {
        return Sets.difference(getClusterAddresses(),
                ImmutableSet.of(install.cluster().localServer()));
    }

    private ClusterConfiguration getClusterConfiguration() {
        return install.cluster();
    }

    private Optional<SslConfiguration> getSslConfigurationOptional() {
        return install.cluster().cluster().security();
    }

    private Set<String> getRemoteServerPaths() {
        return addProtocols(getRemoteServerAddresses());
    }

    private String addProtocol(String address) {
        String protocolPrefix = getSslConfigurationOptional().isPresent() ? "https://" : "http://";
        return protocolPrefix + address;
    }

    private Set<String> addProtocols(Set<String> addresses) {
        return addresses.stream()
                .map(this::addProtocol)
                .collect(Collectors.toSet());
    }
}
