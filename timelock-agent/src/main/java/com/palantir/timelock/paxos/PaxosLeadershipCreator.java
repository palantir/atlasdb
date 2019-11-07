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
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.SetMultimap;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.config.ImmutableLeaderRuntimeConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.config.LeaderRuntimeConfig;
import com.palantir.atlasdb.config.RemotingClientConfigs;
import com.palantir.atlasdb.factory.ImmutableRemotePaxosServerSpec;
import com.palantir.atlasdb.factory.Leaders;
import com.palantir.atlasdb.timelock.paxos.AutobatchingLeadershipObserverFactory;
import com.palantir.atlasdb.timelock.paxos.AutobatchingLeadershipObserverFactory.LeadershipEvent;
import com.palantir.atlasdb.timelock.paxos.Client;
import com.palantir.atlasdb.timelock.paxos.LeadershipResource;
import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants;
import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockUriUtils;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.LeadershipObserver;
import com.palantir.leader.proxy.AwaitingLeadershipProxy;
import com.palantir.timelock.config.PaxosRuntimeConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

class PaxosLeadershipCreator {
    private final MetricsManager metricsManager;
    private final TimeLockInstallConfiguration install;
    private final Supplier<PaxosRuntimeConfiguration> runtime;
    private final Consumer<Object> registrar;

    private LeaderElectionService leaderElectionService;
    private Supplier<Boolean> isCurrentSuspectedLeader;
    private LeaderPingHealthCheck healthCheck;
    private AutobatchingLeadershipObserverFactory leadershipObserverFactory;

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

        leadershipObserverFactory = AutobatchingLeadershipObserverFactory.create(
                leadershipEvents -> deregisterTaggedMetrics(metricsManager, leadershipEvents));

        LeadershipObserver leadershipObserver = leadershipObserverFactory.create(
                PaxosTimeLockConstants.LEGACY_PAXOS_AS_CLIENT);

        Leaders.LocalPaxosServices localPaxosServices = Leaders.createInstrumentedLocalServices(
                metricsManager,
                leaderConfig,
                ImmutableRemotePaxosServerSpec.builder()
                        .remoteLeaderUris(remoteServers)
                        .remoteAcceptorUris(paxosSubresourceUris)
                        .remoteLearnerUris(paxosSubresourceUris)
                        .build(),
                () -> RemotingClientConfigs.ALWAYS_USE_CONJURE,
                UserAgent.of(UserAgent.Agent.of("leader-election-service", UserAgent.Agent.DEFAULT_VERSION)),
                leadershipObserver);
        leaderElectionService = localPaxosServices.leaderElectionService();
        isCurrentSuspectedLeader = localPaxosServices.isCurrentSuspectedLeader();
        healthCheck = new LeaderPingHealthCheck(localPaxosServices.allPingableLeaders());

        registrar.accept(localPaxosServices.localPingableLeader());
        registrar.accept(new LeadershipResource(localPaxosServices.ourAcceptor(), localPaxosServices.ourLearner()));
    }

    <T> T wrapInLeadershipProxy(Supplier<T> delegateSupplier, Class<T> clazz, String client) {
        T instance = AwaitingLeadershipProxy.newProxyInstance(clazz, delegateSupplier, leaderElectionService);
        return instrument(metricsManager.getTaggedRegistry(), clazz, instance, client);
    }

    LeaderPingHealthCheck getHealthCheck() {
        return healthCheck;
    }

    void shutdown() {
        leaderElectionService.markNotEligibleForLeadership();
        leaderElectionService.stepDown();
        leadershipObserverFactory.close();
    }

    private <T> T instrument(TaggedMetricRegistry metricRegistry, Class<T> serviceClass, T service, String client) {
        return AtlasDbMetrics.instrumentWithTaggedMetrics(
                metricRegistry, serviceClass, service, MetricRegistry.name(serviceClass),
                context -> ImmutableMap.of(
                        AtlasDbMetricNames.TAG_CLIENT, client,
                        AtlasDbMetricNames.TAG_CURRENT_SUSPECTED_LEADER, String.valueOf(isCurrentSuspectedLeader())));
    }

    private static void deregisterTaggedMetrics(
            MetricsManager metricsManager,
            SetMultimap<LeadershipEvent, Client> events) {
        events.keySet().forEach(event -> metricsManager
                .deregisterTaggedMetrics(withTagIsCurrentSuspectedLeader(event.isCurrentSuspectedLeader())));
    }

    private static Predicate<MetricName> withTagIsCurrentSuspectedLeader(boolean currentLeader) {
        return metricName ->
                Optional.ofNullable(metricName.safeTags().get(AtlasDbMetricNames.TAG_CURRENT_SUSPECTED_LEADER))
                        .filter(x -> x.equals(String.valueOf(currentLeader)))
                        .isPresent();
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

    private boolean isCurrentSuspectedLeader() {
        return isCurrentSuspectedLeader.get();
    }
}
