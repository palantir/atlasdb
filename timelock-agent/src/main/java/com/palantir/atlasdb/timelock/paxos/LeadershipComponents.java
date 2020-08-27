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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import com.palantir.atlasdb.timelock.paxos.NetworkClientFactories.Factory;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.leader.proxy.AwaitingLeadershipProxy;
import com.palantir.paxos.Client;
import com.palantir.timelock.paxos.HealthCheckPinger;
import com.palantir.timelock.paxos.LeaderPingHealthCheck;
import com.palantir.timelock.paxos.NamespaceTracker;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeadershipComponents {

    private static final Logger log = LoggerFactory.getLogger(LeadershipComponents.class);

    private final ConcurrentMap<Client, LeadershipContext> leadershipContextByClient = Maps.newConcurrentMap();
    private final ShutdownAwareCloser closer = new ShutdownAwareCloser();

    private final Factory<LeadershipContext> leadershipContextFactory;
    private final LocalAndRemotes<HealthCheckPinger> healthCheckPingers;

    LeadershipComponents(
            Factory<LeadershipContext> leadershipContextFactory,
            LocalAndRemotes<HealthCheckPinger> healthCheckPingers) {
        this.leadershipContextFactory = leadershipContextFactory;
        this.healthCheckPingers = healthCheckPingers;
    }

    public <T> T wrapInLeadershipProxy(Client client, Class<T> clazz, Supplier<T> delegateSupplier) {
        LeadershipContext context = getOrCreateNewLeadershipContext(client);
        T instance = AwaitingLeadershipProxy.newProxyInstance(clazz, delegateSupplier, context.leaderElectionService());

        // this is acceptable since the proxy returned implements Closeable and needs to be closed
        Closeable closeableInstance = (Closeable) instance;
        closer.register(closeableInstance);

        return context.leadershipMetrics().instrument(clazz, instance);
    }

    public void shutdown() {
        closer.shutdown();
    }

    public HealthCheckPinger getLocalHealthCheckPinger() {
        return healthCheckPingers.local();
    }

    public LeaderPingHealthCheck healthCheck(NamespaceTracker namespaceTracker) {
        return new LeaderPingHealthCheck(namespaceTracker, healthCheckPingers.all());
    }

    public boolean requestHostileTakeover(Client client) {
        return getOrCreateNewLeadershipContext(client).leaderElectionService().hostileTakeover();
    }

    private LeadershipContext getOrCreateNewLeadershipContext(Client client) {
        return leadershipContextByClient.computeIfAbsent(client, this::createNewLeadershipContext);
    }

    private LeadershipContext createNewLeadershipContext(Client client) {
        LeadershipContext uninstrumentedLeadershipContext = leadershipContextFactory.create(client);
        closer.register(uninstrumentedLeadershipContext.closeables());
        closer.register(() -> shutdownLeaderElectionService(uninstrumentedLeadershipContext.leaderElectionService()));
        return uninstrumentedLeadershipContext;
    }

    private static void shutdownLeaderElectionService(LeaderElectionService leaderElectionService) {
        leaderElectionService.markNotEligibleForLeadership();
        leaderElectionService.stepDown();
    }

    private static class ShutdownAwareCloser {
        private boolean isShutdown = false;
        private final Closer closer = Closer.create();

        synchronized void register(Closeable closeable) {
            register(ImmutableList.of(closeable));
        }

        /**
         * Attempts to register a collection of {@link Closeable}s to be closed when shutting down Timelock.
         * If timelock has already been shutdown, any {@link Closeable}s passed here will be <em>immediately</em>
         * closed, and a {@link NotCurrentLeaderException} will be thrown.
         *
         * @param closeables collection of {@link Closeable}s to be closed when timelock shuts down.
         */
        synchronized void register(Collection<Closeable> closeables) {
            if (isShutdown) {
                ShutdownAwareCloser immediateCloser = new ShutdownAwareCloser();
                immediateCloser.register(closeables);
                immediateCloser.shutdown();
                throw new NotCurrentLeaderException("This timelock node is being shutdown");
            } else {
                closeables.forEach(closer::register);
            }
        }

        /**
         * This is to be called when timelock is shutting down. It will close in LIFO order any resources that were
         * registered during their creation.
         */
        synchronized void shutdown() {
            if (isShutdown) {
                return;
            }

            try {
                closer.close();
            } catch (IOException e) {
                log.warn("Received exceptions whilst trying to shutdown this timelock node.", e);
            } finally {
                isShutdown = true;
            }
        }
    }

    @Value.Immutable
    abstract static class LeadershipContext {
        abstract LeaderElectionService leaderElectionService();
        abstract TimelockLeadershipMetrics leadershipMetrics();
        abstract List<Closeable> closeables();
    }
}
