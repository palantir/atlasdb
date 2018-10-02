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
package com.palantir.atlasdb.timelock.paxos;

import java.nio.file.Paths;
import java.util.Map;

import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import org.immutables.value.Value;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosAcceptorImpl;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerImpl;

@Path("/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE
        + "/" + PaxosTimeLockConstants.CLIENT_PAXOS_NAMESPACE
        + "/{client: [a-zA-Z0-9_-]+}")
public final class PaxosResource {
    private final MetricRegistry metricRegistry;
    private final String logDirectory;
    private final Map<String, PaxosComponents> paxosComponentsByClient = Maps.newConcurrentMap();

    private PaxosResource(MetricRegistry metricRegistry, String logDirectory) {
        this.metricRegistry = metricRegistry;
        this.logDirectory = logDirectory;
    }

    public static PaxosResource create(MetricRegistry metricRegistry, String logDirectory) {
        return new PaxosResource(metricRegistry, logDirectory);
    }

    public PaxosComponents createInstrumentedComponents(String client) {
        String learnerLogDir = Paths.get(logDirectory, client, PaxosTimeLockConstants.LEARNER_SUBDIRECTORY_PATH)
                .toString();
        PaxosLearner learner = instrument(
                metricRegistry,
                PaxosLearner.class,
                PaxosLearnerImpl.newLearner(learnerLogDir),
                client);

        String acceptorLogDir = Paths.get(logDirectory, client, PaxosTimeLockConstants.ACCEPTOR_SUBDIRECTORY_PATH)
                .toString();
        PaxosAcceptor acceptor = instrument(
                metricRegistry,
                PaxosAcceptor.class,
                PaxosAcceptorImpl.newAcceptor(acceptorLogDir),
                client);

        return ImmutablePaxosComponents.builder()
                .acceptor(acceptor)
                .learner(learner)
                .build();
    }

    private static <T> T instrument(MetricRegistry metricRegistry, Class<T> serviceClass, T service, String client) {
        // TODO(nziebart): tag with the client name, when tritium supports it
        return AtlasDbMetrics.instrument(metricRegistry, serviceClass, service, MetricRegistry.name(serviceClass));
    }

    @Path("/learner")
    public PaxosLearner getPaxosLearner(@PathParam("client") String client) {
        return getOrCreateComponents(client).learner();
    }

    @Path("/acceptor")
    public PaxosAcceptor getPaxosAcceptor(@PathParam("client") String client) {
        return getOrCreateComponents(client).acceptor();
    }

    private PaxosComponents getOrCreateComponents(String client) {
        return paxosComponentsByClient.computeIfAbsent(client, this::createInstrumentedComponents);
    }

    @Value.Immutable
    interface PaxosComponents {

        PaxosAcceptor acceptor();

        PaxosLearner learner();
    }
}
