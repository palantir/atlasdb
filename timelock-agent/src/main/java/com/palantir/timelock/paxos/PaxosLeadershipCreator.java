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

import java.nio.file.Paths;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.factory.ImmutableRemotePaxosServerSpec;
import com.palantir.atlasdb.factory.Leaders;
import com.palantir.atlasdb.timelock.paxos.LeadershipResource;
import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants;
import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockUriUtils;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.proxy.AwaitingLeadershipProxy;
import com.palantir.remoting2.config.ssl.SslConfiguration;
import com.palantir.timelock.Observables;
import com.palantir.timelock.config.ClusterConfiguration;
import com.palantir.timelock.config.ImmutablePaxosRuntimeConfiguration;
import com.palantir.timelock.config.PaxosInstallConfiguration;
import com.palantir.timelock.config.PaxosRuntimeConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;

import io.reactivex.Observable;

public class PaxosLeadershipCreator {
    private final TimeLockInstallConfiguration install;
    private final PaxosInstallConfiguration paxosInstall;
    private final Observable<Optional<PaxosRuntimeConfiguration>> runtime;
    private final Consumer<Object> registrar;

    private LeaderElectionService leaderElectionService;

    public PaxosLeadershipCreator(
            TimeLockInstallConfiguration install,
            Observable<TimeLockRuntimeConfiguration> runtime,
            Consumer<Object> registrar) {
        this.install = install;
        Preconditions.checkState(install.algorithm() instanceof PaxosInstallConfiguration,
                "Cannot create a Paxos leadership agent with a non-Paxos install configuration");
        this.paxosInstall = ((PaxosInstallConfiguration) install.algorithm());
        this.runtime = runtime.map(config -> config.algorithm().map(algo -> (PaxosRuntimeConfiguration) algo));
        this.registrar = registrar;
    }

    public void registerLeaderElectionService() {
        Set<String> remoteServers = getRemoteServerPaths();

        LeaderConfig leaderConfig = getLeaderConfig();

        Set<String> paxosSubresourceUris = PaxosTimeLockUriUtils.getLeaderPaxosUris(remoteServers);

        Leaders.LocalPaxosServices localPaxosServices = Leaders.createInstrumentedLocalServices(
                leaderConfig,
                ImmutableRemotePaxosServerSpec.builder()
                        .remoteLeaderUris(remoteServers)
                        .remoteAcceptorUris(paxosSubresourceUris)
                        .remoteLearnerUris(paxosSubresourceUris)
                        .build(),
                "leader-election-service");
        leaderElectionService = localPaxosServices.leaderElectionService();

        registrar.accept(localPaxosServices.pingableLeader());
        registrar.accept(new LeadershipResource(
                localPaxosServices.ourAcceptor(),
                localPaxosServices.ourLearner()));
    }

    public <T> T wrapInLeadershipProxy(Supplier<T> delegateSupplier, Class<T> clazz) {
        return AwaitingLeadershipProxy.newProxyInstance(
                clazz,
                delegateSupplier::get,
                leaderElectionService);
    }

    private LeaderConfig getLeaderConfig() {
        // TODO (jkong): Live Reload Paxos Ping Rates
        PaxosRuntimeConfiguration paxosRuntimeConfiguration = Observables.blockingMostRecent(runtime).get().orElse(
                ImmutablePaxosRuntimeConfiguration.builder().build());
        return ImmutableLeaderConfig.builder()
                .sslConfiguration(getSslConfigurationOptional())
                .leaders(addProtocols(getClusterAddresses()))
                .localServer(addProtocol(getClusterConfiguration().localServer()))
                .acceptorLogDir(Paths.get(paxosInstall.dataDirectory().toString(),
                        PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE,
                        PaxosTimeLockConstants.ACCEPTOR_SUBDIRECTORY_PATH).toFile())
                .learnerLogDir(Paths.get(paxosInstall.dataDirectory().toString(),
                        PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE,
                        PaxosTimeLockConstants.LEARNER_SUBDIRECTORY_PATH).toFile())
                .pingRateMs(paxosRuntimeConfiguration.pingRateMs())
                .quorumSize(getQuorumSize(getClusterAddresses()))
                .leaderPingResponseWaitMs(paxosRuntimeConfiguration.pingRateMs())
                .randomWaitBeforeProposingLeadershipMs(paxosRuntimeConfiguration.pingRateMs())
                .build();
    }

    private static <T> int getQuorumSize(Collection<T> elements) {
        return elements.size() / 2 + 1;
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
