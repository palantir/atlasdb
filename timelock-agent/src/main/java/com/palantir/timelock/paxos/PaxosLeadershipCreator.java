/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.timelock.paxos;

import java.nio.file.Paths;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.config.ImmutableLeaderRuntimeConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.config.LeaderRuntimeConfig;
import com.palantir.atlasdb.factory.ImmutableRemotePaxosServerSpec;
import com.palantir.atlasdb.factory.Leaders;
import com.palantir.atlasdb.timelock.paxos.LeadershipResource;
import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants;
import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockUriUtils;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.LeadershipObserver;
import com.palantir.leader.PingableLeader;
import com.palantir.leader.proxy.AwaitingLeadershipProxy;
import com.palantir.timelock.config.PaxosRuntimeConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;

class PaxosLeadershipCreator {
    private final MetricsManager metricsManager;
    private final TimeLockInstallConfiguration install;
    private final Supplier<PaxosRuntimeConfiguration> runtime;
    private final Consumer<Object> registrar;

    private PingableLeader localPingableLeader;
    private LeaderElectionService leaderElectionService;
    private LeadershipObserver leadershipObserver;
    private Supplier<Boolean> isCurrentSuspectedLeader;

    PaxosLeadershipCreator(
            MetricsManager metricsManager,
            TimeLockInstallConfiguration install,
            Supplier<TimeLockRuntimeConfiguration> runtime,
            Consumer<Object> registrar) {
        this.metricsManager = metricsManager;
        this.install = install;
        this.runtime = Suppliers.compose(TimeLockRuntimeConfiguration::paxos, runtime::get);
        this.registrar = registrar;
    }

    void registerLeaderElectionService() {
        Set<String> remoteServers = PaxosRemotingUtils.getRemoteServerPaths(install);

        LeaderConfig leaderConfig = getLeaderConfig();

        Set<String> paxosSubresourceUris = PaxosTimeLockUriUtils.getLeaderPaxosUris(remoteServers);

        Leaders.LocalPaxosServices localPaxosServices = Leaders.createInstrumentedLocalServices(
                metricsManager,
                leaderConfig,
                Suppliers.compose(getLeaderRuntimeConfig::apply, runtime::get),
                ImmutableRemotePaxosServerSpec.builder()
                        .remoteLeaderUris(remoteServers)
                        .remoteAcceptorUris(paxosSubresourceUris)
                        .remoteLearnerUris(paxosSubresourceUris)
                        .build(),
                UserAgent.of(UserAgent.Agent.of("leader-election-service", UserAgent.Agent.DEFAULT_VERSION)));
        localPingableLeader = localPaxosServices.pingableLeader();
        leaderElectionService = localPaxosServices.leaderElectionService();
        leadershipObserver = localPaxosServices.leadershipObserver();
        isCurrentSuspectedLeader = localPaxosServices.isCurrentSuspectedLeader();

        registrar.accept(localPingableLeader);
        registrar.accept(new LeadershipResource(
                localPaxosServices.ourAcceptor(),
                localPaxosServices.ourLearner()));
    }

    <T> T wrapInLeadershipProxy(Supplier<T> delegateSupplier, Class<T> clazz) {
        return AwaitingLeadershipProxy.newProxyInstance(
                clazz,
                delegateSupplier,
                leaderElectionService);
    }

    private LeaderConfig getLeaderConfig() {
        // TODO (jkong): Live Reload Paxos Ping Rates
        PaxosRuntimeConfiguration paxosRuntimeConfiguration = runtime.get();
        return ImmutableLeaderConfig.builder()
                .sslConfiguration(PaxosRemotingUtils.getSslConfigurationOptional(install))
                .leaders(PaxosRemotingUtils.addProtocols(install, PaxosRemotingUtils.getClusterAddresses(install)))
                .localServer(PaxosRemotingUtils.addProtocol(install,
                        PaxosRemotingUtils.getClusterConfiguration(install).localServer()))
                .acceptorLogDir(Paths.get(install.paxos().dataDirectory().toString(),
                        PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE,
                        PaxosTimeLockConstants.ACCEPTOR_SUBDIRECTORY_PATH).toFile())
                .learnerLogDir(Paths.get(install.paxos().dataDirectory().toString(),
                        PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE,
                        PaxosTimeLockConstants.LEARNER_SUBDIRECTORY_PATH).toFile())
                .pingRateMs(paxosRuntimeConfiguration.pingRateMs())
                .quorumSize(PaxosRemotingUtils.getQuorumSize(PaxosRemotingUtils.getClusterAddresses(install)))
                .leaderPingResponseWaitMs(paxosRuntimeConfiguration.leaderPingResponseWaitMs())
                .randomWaitBeforeProposingLeadershipMs(paxosRuntimeConfiguration.maximumWaitBeforeProposalMs())
                .build();
    }

    private Function<PaxosRuntimeConfiguration, LeaderRuntimeConfig> getLeaderRuntimeConfig =
            config -> ImmutableLeaderRuntimeConfig.builder()
                    .onlyLogOnQuorumFailure(config.onlyLogOnQuorumFailure())
                    .build();

    Supplier<LeaderPingHealthCheck> getHealthCheck() {
        return () -> new LeaderPingHealthCheck(leaderElectionService.getPotentialLeaders());
    }

    void executeWhenGainedLeadership(Runnable task) {
        leadershipObserver.executeWhenGainedLeadership(task);
    }

    void executeWhenLostLeadership(Runnable task) {
        leadershipObserver.executeWhenLostLeadership(task);
    }

    boolean isCurrentSuspectedLeader() {
        return isCurrentSuspectedLeader.get();
    }

    void shutdown() {
        leaderElectionService.markNotEligibleForLeadership();
        leaderElectionService.stepDown();
    }
}
