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

import com.palantir.atlasdb.metrics.Timed;
import com.palantir.common.annotation.Inclusive;
import com.palantir.conjure.java.undertow.annotations.Handle;
import com.palantir.conjure.java.undertow.annotations.HttpMethod;
import com.palantir.paxos.Client;
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
 * Encapsulates a {@link PaxosLearner} but at a defined path for routing purposes.
 */
@Path("/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE
        + "/" + PaxosTimeLockConstants.CLIENT_PAXOS_NAMESPACE
        + "/{client: [a-zA-Z0-9_-]+}"
        + "/learner")
public final class TimestampPaxosLearnerResource {
    private final LocalPaxosComponents paxosComponents;

    public TimestampPaxosLearnerResource(LocalPaxosComponents paxosComponents) {
        this.paxosComponents = paxosComponents;
    }

    @POST
    @Path("learn/{seq:.+}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Timed
    @Handle(
            method = HttpMethod.POST,
            path = "/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE
                    + "/" + PaxosTimeLockConstants.CLIENT_PAXOS_NAMESPACE
                    + "/{client}/learner/learn/{seq}")
    public void learn(
            @PathParam("client") @Handle.PathParam String client,
            @PathParam("seq") @Handle.PathParam long seq,
            @Handle.Body PaxosValue val) {
        getLearner(client).learn(seq, val);
    }

    @GET
    @Path("learned-value/{seq:.+}")
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    @Handle(
            method = HttpMethod.GET,
            path = "/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE
                    + "/" + PaxosTimeLockConstants.CLIENT_PAXOS_NAMESPACE
                    + "/{client}/learner/learned-value/{seq}")
    public Optional<PaxosValue> getLearnedValue(
            @PathParam("client") @Handle.PathParam String client, @PathParam("seq") @Handle.PathParam long seq) {
        return getLearner(client).getLearnedValue(seq);
    }

    @GET
    @Path("greatest-learned-value")
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    @Handle(
            method = HttpMethod.GET,
            path = "/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE
                    + "/" + PaxosTimeLockConstants.CLIENT_PAXOS_NAMESPACE
                    + "/{client}/learner/greatest-learned-value")
    public Optional<PaxosValue> getGreatestLearnedValue(@PathParam("client") @Handle.PathParam String client) {
        return getLearner(client).getGreatestLearnedValue();
    }

    @GET
    @Path("learned-values-since/{seq:.+}")
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    @Handle(
            method = HttpMethod.GET,
            path = "/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE
                    + "/" + PaxosTimeLockConstants.CLIENT_PAXOS_NAMESPACE
                    + "/{client}/learner/learned-values-since/{seq}")
    public Collection<PaxosValue> getLearnedValuesSince(
            @PathParam("client") @Handle.PathParam String client,
            @PathParam("seq") @Handle.PathParam @Inclusive long seq) {
        return getLearner(client).getLearnedValuesSince(seq);
    }

    private PaxosLearner getLearner(String client) {
        return paxosComponents.learner(Client.of(client));
    }
}
