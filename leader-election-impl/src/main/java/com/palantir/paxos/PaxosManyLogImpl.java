/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.paxos;

import java.io.File;
import java.util.Collection;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

public class PaxosManyLogImpl implements PaxosManyLogApi {
    final Map<String, PaxosAcceptor> acceptors;
    final Map<String, PaxosLearner> learners;

    public static PaxosManyLogApi create(Map<String, String> logDirectories) {
        Builder<String, PaxosAcceptor> acceptors = ImmutableMap.builder();
        Builder<String, PaxosLearner> learners = ImmutableMap.builder();
        for (Map.Entry<String, String> e : logDirectories.entrySet()) {
            acceptors.put(e.getKey(), PaxosAcceptorImpl.newAcceptor(new File(e.getValue(), "acceptor-log")));
            learners.put(e.getKey(), PaxosLearnerImpl.newLearner(new File(e.getValue(), "learner-log")));
        }
        return new PaxosManyLogImpl(acceptors.build(), learners.build());
    }

    private PaxosManyLogImpl(Map<String, PaxosAcceptor> acceptors,
                             Map<String, PaxosLearner> learners) {
        this.acceptors = acceptors;
        this.learners = learners;
    }

    @Override
    public void learn(String logName, long seq, PaxosValue val) {
        learners.get(logName).learn(seq, val);
    }

    @Override
    public PaxosValue getLearnedValue(String logName, long seq) {
        return learners.get(logName).getLearnedValue(seq);
    }

    @Override
    public PaxosValue getGreatestLearnedValue(String logName) {
        return learners.get(logName).getGreatestLearnedValue();
    }

    @Override
    public Collection<PaxosValue> getLearnedValuesSince(String logName, long seq) {
        return learners.get(logName).getLearnedValuesSince(seq);
    }

    @Override
    public PaxosPromise prepare(String logName, long seq, PaxosProposalId pid) {
        return acceptors.get(logName).prepare(seq, pid);
    }

    @Override
    public BooleanPaxosResponse accept(String logName, long seq, PaxosProposal proposal) {
        return acceptors.get(logName).accept(seq, proposal);
    }

    @Override
    public long getLatestSequencePreparedOrAccepted(String logName) {
        return acceptors.get(logName).getLatestSequencePreparedOrAccepted();
    }

}
