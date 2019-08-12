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

import org.immutables.value.Value;

import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public class ClientPaxosResourceFactory {

    private ClientPaxosResourceFactory() { }

    public static ClientResources create(TaggedMetricRegistry metricRegistry, Path logDirectory) {
        PaxosComponents paxosComponents = new PaxosComponents(metricRegistry, "bound-store", logDirectory);
        PaxosResource paxosResource = new PaxosResource(paxosComponents);
        BatchPaxosAcceptor acceptorResource = new BatchPaxosAcceptorResource(paxosComponents, new AcceptorCacheImpl());
        BatchPaxosLearner learnerResource = new BatchPaxosLearnerResource(paxosComponents);
        BatchPaxosResource batchPaxosResource = new BatchPaxosResource(acceptorResource, learnerResource);
        return ImmutableClientResources.builder()
                .nonBatchedResource(paxosResource)
                .batchedResource(batchPaxosResource)
                .build();
    }

    @Value.Immutable
    public interface ClientResources {
        PaxosResource nonBatchedResource();
        BatchPaxosResource batchedResource();
    }
}
