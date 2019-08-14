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
import java.nio.file.Paths;
import java.util.Map;

import org.immutables.value.Value;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosAcceptorImpl;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerImpl;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public class PaxosComponents {

    private final TaggedMetricRegistry metrics;
    private final String useCase;
    private final Path logDirectory;
    private final Map<Client, Components> componentsByClient = Maps.newConcurrentMap();

    PaxosComponents(TaggedMetricRegistry metrics, String useCase, Path logDirectory) {
        this.metrics = metrics;
        this.useCase = useCase;
        this.logDirectory = logDirectory;
    }

    public PaxosAcceptor acceptor(Client client) {
        return getOrCreateComponents(client).acceptor();
    }

    public PaxosLearner learner(Client client) {
        return getOrCreateComponents(client).learner();
    }

    private Components getOrCreateComponents(Client client) {
        return componentsByClient.computeIfAbsent(client, this::createComponents);
    }

    private Components createComponents(Client client) {
        Path clientDirectory = logDirectory.resolve(client.value());
        Path learnerLogDir = Paths.get(clientDirectory.toString(), PaxosTimeLockConstants.LEARNER_SUBDIRECTORY_PATH);

        PaxosLearner learner = instrument(
                PaxosLearner.class,
                PaxosLearnerImpl.newLearner(learnerLogDir.toString()),
                "timelock.paxos-learner",
                client);

        Path acceptorLogDir = Paths.get(clientDirectory.toString(), PaxosTimeLockConstants.ACCEPTOR_SUBDIRECTORY_PATH);
        PaxosAcceptor acceptor = instrument(
                PaxosAcceptor.class,
                PaxosAcceptorImpl.newAcceptor(acceptorLogDir.toString()),
                "timelock.paxos-acceptor",
                client);

        return ImmutableComponents.builder()
                .acceptor(acceptor)
                .learner(learner)
                .build();
    }

    private <T, U extends T> T instrument(Class<T> clazz, U instance, String name, Client client) {
        ImmutableMap<String, String> tags = ImmutableMap.<String, String>builder()
                .put("client", client.value())
                .put("useCase", useCase)
                .build();
        return AtlasDbMetrics.instrumentWithTaggedMetrics(
                metrics,
                clazz,
                instance,
                name,
                unused -> tags);
    }

    @Value.Immutable
    interface Components {
        PaxosAcceptor acceptor();
        PaxosLearner learner();
    }

}
