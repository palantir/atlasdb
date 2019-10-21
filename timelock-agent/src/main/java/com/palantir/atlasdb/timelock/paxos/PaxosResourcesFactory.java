/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.paxos;

import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import org.immutables.value.Value;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.timelock.paxos.NetworkClientFactories.Factory;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.proxy.PredicateSwitchedProxy;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.paxos.PaxosAcceptorNetworkClient;
import com.palantir.paxos.PaxosLearnerNetworkClient;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosProposerImpl;
import com.palantir.timelock.config.PaxosRuntimeConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.paxos.PaxosRemotingUtils;

public final class PaxosResourcesFactory {

    private PaxosResourcesFactory() { }

    public static PaxosResources create(
            TimelockPaxosInstallationContext install,
            MetricsManager metrics,
            Supplier<PaxosRuntimeConfiguration> paxosRuntime,
            ExecutorService sharedExecutor) {
        PaxosRemoteClients remoteClients = ImmutablePaxosRemoteClients.of(install, metrics.getTaggedRegistry());

        PaxosUseCaseContext timestampContext =
                timestampContext(install, metrics, paxosRuntime, sharedExecutor, remoteClients);

        return ImmutablePaxosResources.builder()
                .timestamp(timestampContext)
                .addAdhocResources(new TimestampPaxosResource(timestampContext.components()))
                .build();
    }

    private static PaxosUseCaseContext timestampContext(
            TimelockPaxosInstallationContext install,
            MetricsManager metrics,
            Supplier<PaxosRuntimeConfiguration> paxosRuntime,
            ExecutorService sharedExecutor,
            PaxosRemoteClients remoteClients) {

        PaxosUseCaseContext timestampBatchContext = batchUseCaseContext(
                install,
                remoteClients,
                metrics,
                PaxosUseCase.TIMESTAMP,
                sharedExecutor);

        NetworkClientFactories singleLeaderClientFactories = ImmutableSingleLeaderNetworkClientFactories.builder()
                .useCase(PaxosUseCase.TIMESTAMP)
                .metrics(timestampBatchContext.metrics())
                .remoteClients(remoteClients)
                .components(timestampBatchContext.components())
                .quorumSize(install.quorumSize())
                .sharedExecutor(sharedExecutor)
                .build();

        Supplier<Boolean> useBatchPaxosForTimestamps = Suppliers.compose(
                runtime -> runtime.timestampPaxos().useBatchPaxos(), paxosRuntime::get);

        NetworkClientFactories combinedNetworkClientFactories = ImmutableNetworkClientFactories.builder()
                .acceptor(client -> PredicateSwitchedProxy.newProxyInstance(
                        timestampBatchContext.networkClientFactories().acceptor().create(client),
                        singleLeaderClientFactories.acceptor().create(client),
                        useBatchPaxosForTimestamps,
                        PaxosAcceptorNetworkClient.class))
                .learner(client -> PredicateSwitchedProxy.newProxyInstance(
                        timestampBatchContext.networkClientFactories().learner().create(client),
                        singleLeaderClientFactories.learner().create(client),
                        useBatchPaxosForTimestamps,
                        PaxosLearnerNetworkClient.class))
                .addAllCloseables(timestampBatchContext.networkClientFactories().closeables())
                .addAllCloseables(singleLeaderClientFactories.closeables())
                .build();

        return ImmutablePaxosUseCaseContext.builder()
                .from(timestampBatchContext)
                .networkClientFactories(combinedNetworkClientFactories)
                .build();
    }

    private static PaxosUseCaseContext batchUseCaseContext(
            TimelockPaxosInstallationContext install,
            PaxosRemoteClients remoteClients,
            MetricsManager metrics,
            PaxosUseCase useCase,
            ExecutorService sharedExecutor) {
        TimelockPaxosMetrics timelockMetrics = TimelockPaxosMetrics.of(useCase, metrics.getTaggedRegistry());

        LocalPaxosComponents paxosComponents = new LocalPaxosComponents(
                timelockMetrics,
                useCase.logDirectoryRelativeToDataDirectory(install.dataDirectory()));

        NetworkClientFactories batchClientFactories = ImmutableBatchingNetworkClientFactories.builder()
                .useCase(useCase)
                .metrics(timelockMetrics)
                .remoteClients(remoteClients)
                .components(paxosComponents)
                .quorumSize(install.quorumSize())
                .sharedExecutor(sharedExecutor)
                .build();

        return ImmutablePaxosUseCaseContext.builder()
                .install(install)
                .metrics(timelockMetrics)
                .components(paxosComponents)
                .networkClientFactories(batchClientFactories)
                .build();
    }

    @Value.Immutable
    public interface PaxosUseCaseContext {
        TimelockPaxosInstallationContext install();
        TimelockPaxosMetrics metrics();
        LocalPaxosComponents components();
        NetworkClientFactories networkClientFactories();

        @Value.Derived
        default Factory<PaxosProposer> proposerFactory() {
            return client -> {
                PaxosAcceptorNetworkClient acceptorNetworkClient = networkClientFactories().acceptor().create(client);
                PaxosLearnerNetworkClient learnerNetworkClient = networkClientFactories().learner().create(client);

                PaxosProposer paxosProposer = PaxosProposerImpl.newProposer(
                        acceptorNetworkClient,
                        learnerNetworkClient,
                        install().nodeUuid());

                return metrics().instrument(PaxosProposer.class, paxosProposer, "paxos-proposer", client);
            };
        }
    }

    @Value.Immutable
    public interface TimelockPaxosInstallationContext {

        @Value.Parameter
        TimeLockInstallConfiguration install();

        @Value.Derived
        default UUID nodeUuid() {
            return UUID.randomUUID();
        }

        @Value.Derived
        default int quorumSize() {
            return PaxosRemotingUtils.getQuorumSize(clusterAddresses());
        }

        @Value.Derived
        default Set<String> clusterAddresses() {
            return PaxosRemotingUtils.getClusterAddresses(install());
        }

        @Value.Derived
        default Set<String> remoteUris() {
            return PaxosRemotingUtils.getRemoteServerPaths(install());
        }

        @Value.Derived
        default Path dataDirectory() {
            return install().paxos().dataDirectory().toPath();
        }

        @Value.Derived
        default Optional<TrustContext> trustContext() {
            return PaxosRemotingUtils
                    .getSslConfigurationOptional(install())
                    .map(SslSocketFactories::createTrustContext);
        }

    }

}
