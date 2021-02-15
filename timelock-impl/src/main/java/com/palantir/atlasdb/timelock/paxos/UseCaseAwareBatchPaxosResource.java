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
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

@Path("/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE)
public class UseCaseAwareBatchPaxosResource {

    private final EnumMap<PaxosUseCase, BatchPaxosResources> resourcesByUseCase;

    UseCaseAwareBatchPaxosResource(EnumMap<PaxosUseCase, BatchPaxosResources> resourcesByUseCase) {
        this.resourcesByUseCase = resourcesByUseCase;
    }

    @Path("{useCase}/" + PaxosTimeLockConstants.BATCH_INTERNAL_NAMESPACE + "/acceptor")
    public BatchPaxosAcceptorResource acceptor(@PathParam("useCase") PaxosUseCase useCase) {
        return getResourcesForUseCase(useCase).batchAcceptor();
    }

    @Path("{useCase}/" + PaxosTimeLockConstants.BATCH_INTERNAL_NAMESPACE + "/learner")
    public BatchPaxosLearnerResource learner(@PathParam("useCase") PaxosUseCase useCase) {
        return getResourcesForUseCase(useCase).batchLearner();
    }

    private BatchPaxosResources getResourcesForUseCase(PaxosUseCase useCase) {
        BatchPaxosResources resourcesForUseCase = resourcesByUseCase.get(useCase);
        if (resourcesForUseCase == null) {
            throw new NotFoundException();
        }
        return resourcesForUseCase;
    }
}
