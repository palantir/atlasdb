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

package com.palantir.timelock.paxos;

import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants;
import com.palantir.atlasdb.timelock.paxos.PaxosUseCase;
import com.palantir.common.annotation.Inclusive;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosValue;
import java.util.Collection;
import java.util.Optional;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * This interface is used internally by timelock to allow creating a single feign proxy where different clients and
 * different paxos use cases can be injected using {@link TimelockPaxosLearnerAdapter} to create
 * {@link PaxosLearner}s instead of creating a proxy per client and per use case. This reduces the resource allocation
 * when a new timelock node becomes the leader.
 */
@Path("/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE
        + "/{useCase}"
        + "/{client}"
        + "/learner")
public interface TimelockPaxosLearnerRpcClient {
    @POST
    @Path("learn/{seq}")
    @Consumes(MediaType.APPLICATION_JSON)
    void learn(
            @PathParam("useCase") PaxosUseCase paxosUseCase,
            @PathParam("client") String client,
            @PathParam("seq") long seq,
            PaxosValue val);

    @GET
    @Path("learned-value/{seq}")
    @Produces(MediaType.APPLICATION_JSON)
    Optional<PaxosValue> getLearnedValue(
            @PathParam("useCase") PaxosUseCase paxosUseCase,
            @PathParam("client") String client,
            @PathParam("seq") long seq);

    @GET
    @Path("greatest-learned-value")
    @Produces(MediaType.APPLICATION_JSON)
    Optional<PaxosValue> getGreatestLearnedValue(
            @PathParam("useCase") PaxosUseCase paxosUseCase,
            @PathParam("client") String client);

    @GET
    @Path("learned-values-since/{seq}")
    @Produces(MediaType.APPLICATION_JSON)
    Collection<PaxosValue> getLearnedValuesSince(
            @PathParam("useCase") PaxosUseCase paxosUseCase,
            @PathParam("client") String client,
            @PathParam("seq") @Inclusive long seq);
}
