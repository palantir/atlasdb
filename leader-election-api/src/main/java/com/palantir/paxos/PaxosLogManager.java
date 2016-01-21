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
