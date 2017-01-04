/**
 * Copyright 2017 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
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
import com.palantir.paxos.PaxosAcceptorImpl;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerImpl;

@Path("/{client: [a-zA-Z0-9_-]+}")
public class PaxosResource {
    private static final String DEFAULT_LOG_DIRECTORY = "var/log/";
    private static final String LEARNER_PATH = "/learner";
    private static final String ACCEPTOR_PATH = "/acceptor";

    private final PaxosLearner paxosLearner;
    private final PaxosAcceptor paxosAcceptor;

    public PaxosResource(PaxosLearner paxosLearner, PaxosAcceptor paxosAcceptor) {
        this.paxosLearner = paxosLearner;
        this.paxosAcceptor = paxosAcceptor;
    }

    public static PaxosResource createPaxosResourceForClient(String client) {
        String rootSubdirectory = DEFAULT_LOG_DIRECTORY + client;
        PaxosLearner paxosLearner = PaxosLearnerImpl.newLearner(rootSubdirectory + LEARNER_PATH);
        PaxosAcceptor paxosAcceptor = PaxosAcceptorImpl.newAcceptor(rootSubdirectory + ACCEPTOR_PATH);
        return new PaxosResource(paxosLearner, paxosAcceptor);
    }

    @Path("/learner")
    public PaxosLearner getPaxosLearner() {
        return paxosLearner;
    }

    @Path("/acceptor")
    public PaxosAcceptor getPaxosAcceptor() {
        return paxosAcceptor;
    }
}
