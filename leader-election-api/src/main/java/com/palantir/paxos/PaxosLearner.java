/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.paxos;

import com.palantir.atlasdb.metrics.Timed;
import com.palantir.common.annotation.Inclusive;
import java.util.Collection;
import java.util.Optional;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/learner")
public interface PaxosLearner {

    /**
     * Learn given value for the seq-th round.
     *
     * @param seq round in question
     * @param val value learned for that round
     */
    @POST
    @Path("learn/{seq:.+}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Timed
    void learn(@PathParam("seq") long seq, PaxosValue val);

    /**
     * Returns the learned value at the specified sequence number, or {@link Optional#empty()} if the learner
     * does not know such a value.
     */
    @GET
    @Path("learned-value/{seq:.+}")
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    Optional<PaxosValue> getLearnedValue(@PathParam("seq") long seq);

    /**
     * Returns the learned value for the greatest known round or {@link Optional#empty()} if nothing has been learned.
     */
    @GET
    @Path("greatest-learned-value")
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    Optional<PaxosValue> getGreatestLearnedValue();

    /**
     * Returns some collection of learned values since the seq-th round (inclusive).
     *
     * @param seq lower round cutoff for returned values
     * @return some set of learned values for rounds since the seq-th round
     */
    @GET
    @Path("learned-values-since/{seq:.+}")
    @Produces(MediaType.APPLICATION_JSON)
    @Timed
    Collection<PaxosValue> getLearnedValuesSince(@PathParam("seq") @Inclusive long seq);

}
