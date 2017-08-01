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

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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
    private final String logDirectory;
    private final Map<String, PaxosLearner> paxosLearners;
    private final Map<String, PaxosAcceptor> paxosAcceptors;
    private final Map<String, LeadershipResource> paxosLeaders;

    private PaxosResource(String logDirectory,
            Map<String, PaxosLearner> paxosLearners,
            Map<String, PaxosAcceptor> paxosAcceptors,
            Map<String, LeadershipResource> paxosLeaders) {
        this.logDirectory = logDirectory;
        this.paxosLearners = paxosLearners;
        this.paxosAcceptors = paxosAcceptors;
        this.paxosLeaders = paxosLeaders;
    }

    public static PaxosResource create() {
        return create(PaxosTimeLockConstants.DEFAULT_LOG_DIRECTORY);
    }

    public static PaxosResource create(String logDirectory) {
        return new PaxosResource(logDirectory,
                Maps.newConcurrentMap(),
                Maps.newConcurrentMap(),
                Maps.newConcurrentMap());
    }

    @VisibleForTesting
    void addInstrumentedClient(String client) {
        Preconditions.checkState(!paxosLearners.containsKey(client),
                "Paxos resource already has client '%s' registered", client);

        String learnerLogDir = Paths.get(logDirectory, client, PaxosTimeLockConstants.LEARNER_SUBDIRECTORY_PATH)
                .toString();
        PaxosLearner learner = instrument(
                PaxosLearner.class,
                PaxosLearnerImpl.newLearner(learnerLogDir),
                client);
        paxosLearners.put(client, learner);

        String acceptorLogDir = Paths.get(logDirectory, client, PaxosTimeLockConstants.ACCEPTOR_SUBDIRECTORY_PATH)
                .toString();
        PaxosAcceptor acceptor = instrument(
                PaxosAcceptor.class,
                PaxosAcceptorImpl.newAcceptor(acceptorLogDir),
                client);
        paxosAcceptors.put(client, acceptor);
    }

    public void addInstrumentedClient(String client, LeadershipResource resource) {
        String learnerLogDir = Paths.get(logDirectory, client, PaxosTimeLockConstants.LEARNER_SUBDIRECTORY_PATH)
                .toString();
        PaxosLearner learner = instrument(
                PaxosLearner.class,
                PaxosLearnerImpl.newLearner(learnerLogDir),
                client);
        paxosLearners.put(client, learner);

        String acceptorLogDir = Paths.get(logDirectory, client, PaxosTimeLockConstants.ACCEPTOR_SUBDIRECTORY_PATH)
                .toString();
        PaxosAcceptor acceptor = instrument(
                PaxosAcceptor.class,
                PaxosAcceptorImpl.newAcceptor(acceptorLogDir),
                client);
        paxosAcceptors.put(client, acceptor);

        paxosLeaders.put(client, resource);
    }

    private static <T> T instrument(Class<T> serviceClass, T service, String client) {
        return AtlasDbMetrics.instrument(serviceClass, service, MetricRegistry.name(serviceClass, client));
    }

    @Path("/learner")
    public PaxosLearner getPaxosLearner(@PathParam("client") String client) {
        return paxosLearners.get(client);
    }

    @Path("/acceptor")
    public PaxosAcceptor getPaxosAcceptor(@PathParam("client") String client) {
        return paxosAcceptors.get(client);
    }

    @Path("/leadership")
    public LeadershipResource getLeadershipResource(@PathParam("client") String client) {
        return paxosLeaders.get(client);
    }
}
