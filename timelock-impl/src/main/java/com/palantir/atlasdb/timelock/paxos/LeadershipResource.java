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
package com.palantir.atlasdb.timelock.paxos;

import javax.ws.rs.Path;

import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;

@Path("/" + PaxosTimeLockConstants.INTERNAL_NAMESPACE
        + "/" + PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE)
public class LeadershipResource {
    private final PaxosAcceptor acceptor;
    private final PaxosLearner learner;

    public LeadershipResource(
            PaxosAcceptor acceptor,
            PaxosLearner learner) {
        this.acceptor = acceptor;
        this.learner = learner;
    }

    @Path("/acceptor")
    public PaxosAcceptor getAcceptor() {
        return acceptor;
    }

    @Path("/learner")
    public PaxosLearner getLearner() {
        return learner;
    }
}
