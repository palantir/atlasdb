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
import com.google.common.collect.ImmutableMap;
import com.palantir.common.streams.KeyedStream;
import com.palantir.timestamp.ManagedTimestampService;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.immutables.value.Value;

@Value.Immutable
public abstract class PaxosResources {

    public abstract NetworkClientFactories.Factory<ManagedTimestampService> timestampServiceFactory();

    abstract LocalPaxosComponents timestampPaxosComponents();

    abstract Map<PaxosUseCase, LocalPaxosComponents> leadershipBatchComponents();

    public abstract LeadershipContextFactory leadershipContextFactory();

    abstract List<Object> adhocResources();

    public abstract TimeLockCorruptionComponents timeLockCorruptionComponents();

    @Value.Derived
    Map<PaxosUseCase, BatchPaxosResources> leadershipBatchResources() {
        return KeyedStream.stream(leadershipBatchComponents())
                .map(PaxosResources::batchResourcesFromComponents)
                .collectToMap();
    }

    @Value.Derived
    public List<Object> resourcesForRegistration() {
        Map<PaxosUseCase, BatchPaxosResources> batchPaxosResourcesByUseCase =
                ImmutableMap.<PaxosUseCase, BatchPaxosResources>builder()
                        .put(PaxosUseCase.TIMESTAMP, batchResourcesFromComponents(timestampPaxosComponents()))
                        .putAll(leadershipBatchResources())
                        .build();

        UseCaseAwareBatchPaxosResource combinedBatchResource =
                new UseCaseAwareBatchPaxosResource(new EnumMap<>(batchPaxosResourcesByUseCase));

        return ImmutableList.builder()
                .addAll(adhocResources())
                .add(combinedBatchResource)
                .add(new NamespaceTakeoverResource(leadershipComponents()))
                .build();
    }

    @Value.Derived
    public LeadershipComponents leadershipComponents() {
        return new LeadershipComponents(
                leadershipContextFactory(), leadershipContextFactory().healthCheckPingers());
    }

    private static BatchPaxosResources batchResourcesFromComponents(LocalPaxosComponents components) {
        BatchPaxosAcceptorResource acceptorResource = new BatchPaxosAcceptorResource(components.batchAcceptor());
        BatchPaxosLearnerResource learnerResource = new BatchPaxosLearnerResource(components.batchLearner());
        return ImmutableBatchPaxosResources.of(acceptorResource, learnerResource);
    }
}
