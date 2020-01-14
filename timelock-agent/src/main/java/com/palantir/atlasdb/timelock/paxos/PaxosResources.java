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

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.immutables.value.Value;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@Value.Immutable
public abstract class PaxosResources {
    public abstract PaxosResourcesFactory.PaxosUseCaseContext timestamp();
    abstract List<Object> adhocResources();

    abstract LeadershipContextFactory leadershipContextFactory();

    @Value.Derived
    public List<Object> resourcesForRegistration() {
        Map<PaxosUseCase, BatchPaxosResources> batchPaxosResourcesByUseCase =
                ImmutableMap.<PaxosUseCase, BatchPaxosResources>builder()
                .put(PaxosUseCase.TIMESTAMP, batchResourcesForUseCase(timestamp()))
                .build();

        UseCaseAwareBatchPaxosResource combinedBatchResource =
                new UseCaseAwareBatchPaxosResource(new EnumMap<>(batchPaxosResourcesByUseCase));

        return ImmutableList.builder()
                .addAll(adhocResources())
                .add(combinedBatchResource)
                .build();
    }

    @Value.Derived
    public LeadershipComponents leadershipComponents() {
        return new LeadershipComponents(leadershipContextFactory(), leadershipContextFactory().leaderPingHealthCheck());
    }

    private static BatchPaxosResources batchResourcesForUseCase(
            PaxosResourcesFactory.PaxosUseCaseContext useCaseContext) {
        LocalPaxosComponents components = useCaseContext.components();
        BatchPaxosAcceptorResource acceptorResource = new BatchPaxosAcceptorResource(components.batchAcceptor());
        BatchPaxosLearnerResource learnerResource = new BatchPaxosLearnerResource(components.batchLearner());
        return ImmutableBatchPaxosResources.of(acceptorResource, learnerResource);
    }
}
