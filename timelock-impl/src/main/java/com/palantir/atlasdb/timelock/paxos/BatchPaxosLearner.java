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

import java.util.Map;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.common.collect.SetMultimap;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosValue;

@Path("/batch/learner")
public interface BatchPaxosLearner {

    /**
     * Batch counterpart to {@link PaxosLearner#learn}. For a given {@link Client} on paxos instance {@code seq},
     * the learner learns the given {@link PaxosValue}.
     * <p>
     * @param paxosValuesByClientAndSeq {@link ClientAndSeq} identifies the client and the instance of paxos that is
     * being taught; {@link PaxosValue} - for the above {@link ClientAndSeq} - is value being taught.
     */
    @POST
    @Path("learn")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    void learn(Map<ClientAndSeq, PaxosValue> paxosValuesByClientAndSeq);

    /**
     * Batch counterpart to {@link PaxosLearner#getLearnedValue}. For a given {@link Client} on paxos instance
     * {@code seq}, it returns the learnt value. Values where nothing has been learnt are included as a separate field.
     * <p>
     * @param clientAndSeqs the {@link ClientAndSeq}'s - which identifies the client and the instance of paxos - to
     * retrieve the learnt values for.
     * @return for each {@link ClientAndSeq}, the {@link PaxosValue} learnt including {@link ClientAndSeq}'s where
     * nothing was learnt.
     */
    @POST
    @Path("learned-values")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    PaxosLearnedValuesResponse getLearnedValues(Set<ClientAndSeq> clientAndSeqs);

    /**
     * Batch counterpart to {@link PaxosLearner#getLearnedValuesSince}. For a given {@link Client}, returns all learnt
     * values since the minimum provided seq-th round (inclusive).
     * <p>
     * @param seqLowerBoundsByClient for each {@link Client}, the lower bound for the seq-th paxos round to fetch all
     * learnt paxos values since that paxos round.
     * @return for each {@link Client}, all learnt {@link PaxosValue}'s past the given lower bound
     */
    @POST
    @Path("learned-values-since")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    SetMultimap<Client, PaxosValue> getLearnedValuesSince(Map<Client, Long> seqLowerBoundsByClient);

}
