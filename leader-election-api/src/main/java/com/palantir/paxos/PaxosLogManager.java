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

import java.util.Collection;

public class PaxosLogManager {
    private final PaxosManyLogApi api;

    public PaxosLogManager(PaxosManyLogApi api) {
        this.api = api;
    }

    public PaxosAcceptor getAcceptor(final String logName) {
        return new PaxosAcceptor() {

            @Override
            public PaxosPromise prepare(long seq, PaxosProposalId pid) {
                return api.prepare(logName, seq, pid);
            }

            @Override
            public long getLatestSequencePreparedOrAccepted() {
                return api.getLatestSequencePreparedOrAccepted(logName);
            }

            @Override
            public BooleanPaxosResponse accept(long seq, PaxosProposal proposal) {
                return api.accept(logName, seq, proposal);
            }
        };
    }

    public PaxosLearner getLearner(final String logName) {
        return new PaxosLearner() {

            @Override
            public void learn(long seq, PaxosValue val) {
                api.learn(logName, seq, val);
            }

            @Override
            public Collection<PaxosValue> getLearnedValuesSince(long seq) {
                return api.getLearnedValuesSince(logName, seq);
            }

            @Override
            public PaxosValue getLearnedValue(long seq) {
                return api.getLearnedValue(logName, seq);
            }

            @Override
            public PaxosValue getGreatestLearnedValue() {
                return api.getGreatestLearnedValue(logName);
            }
        };
    }

}
