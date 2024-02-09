/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.collect.SetMultimap;
import com.palantir.conjure.java.undertow.annotations.Handle;
import com.palantir.conjure.java.undertow.annotations.HttpMethod;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosValue;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE
        + "/{useCase}/"
        + PaxosTimeLockConstants.BATCH_INTERNAL_NAMESPACE
        + "/learner")
public final class UseCaseAwareBatchPaxosLearnerResource {
    private final Function<PaxosUseCase, BatchPaxosLearnerResource> delegate;

    public UseCaseAwareBatchPaxosLearnerResource(Function<PaxosUseCase, BatchPaxosLearnerResource> delegate) {
        this.delegate = delegate;
    }

    @POST
    @Path("learn")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Handle(
            method = HttpMethod.POST,
            path = "/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE + "/{useCase}/"
                    + PaxosTimeLockConstants.BATCH_INTERNAL_NAMESPACE + "/learner/learn")
    public void learn(
            @PathParam("useCase") @Handle.PathParam PaxosUseCase useCase,
            @Handle.Body SetMultimap<Client, PaxosValue> paxosValuesByClient) {
        getBatchLearner(useCase).learn(paxosValuesByClient);
    }

    @POST
    @Path("learned-values")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Handle(
            method = HttpMethod.POST,
            path = "/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE + "/{useCase}/"
                    + PaxosTimeLockConstants.BATCH_INTERNAL_NAMESPACE + "/learner/learned-values")
    public SetMultimap<Client, PaxosValue> getLearnedValues(
            @PathParam("useCase") @Handle.PathParam PaxosUseCase useCase,
            @Handle.Body Set<WithSeq<Client>> clientAndSeqs) {
        return getBatchLearner(useCase).getLearnedValues(clientAndSeqs);
    }

    @POST
    @Path("learned-values-since")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Handle(
            method = HttpMethod.POST,
            path = "/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE + "/{useCase}/"
                    + PaxosTimeLockConstants.BATCH_INTERNAL_NAMESPACE + "/learner/learned-values-since")
    public SetMultimap<Client, PaxosValue> getLearnedValuesSince(
            @PathParam("useCase") @Handle.PathParam PaxosUseCase useCase,
            @Handle.Body Map<Client, Long> seqLowerBoundsByClient) {
        return getBatchLearner(useCase).getLearnedValuesSince(seqLowerBoundsByClient);
    }

    private BatchPaxosLearnerResource getBatchLearner(PaxosUseCase useCase) {
        return delegate.apply(useCase);
    }
}
