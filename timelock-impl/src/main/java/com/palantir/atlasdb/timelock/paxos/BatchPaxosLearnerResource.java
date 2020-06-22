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

import com.google.common.collect.SetMultimap;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosValue;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/" + PaxosTimeLockConstants.BATCH_INTERNAL_NAMESPACE
        + "/learner")
public class BatchPaxosLearnerResource {

    private final BatchPaxosLearner batchPaxosLearner;

    public BatchPaxosLearnerResource(BatchPaxosLearner batchPaxosLearner) {
        this.batchPaxosLearner = batchPaxosLearner;
    }

    @POST
    @Path("learn")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public void learn(SetMultimap<Client, PaxosValue> paxosValuesByClient) {
        batchPaxosLearner.learn(paxosValuesByClient);
    }

    @POST
    @Path("learned-values")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public SetMultimap<Client, PaxosValue> getLearnedValues(Set<WithSeq<Client>> clientAndSeqs) {
        return batchPaxosLearner.getLearnedValues(clientAndSeqs);
    }

    @POST
    @Path("learned-values-since")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public SetMultimap<Client, PaxosValue> getLearnedValuesSince(Map<Client, Long> seqLowerBoundsByClient) {
        return batchPaxosLearner.getLearnedValuesSince(seqLowerBoundsByClient);
    }

}
