package com.palantir.paxos.learner;

import java.util.Collection;

import com.google.common.collect.ImmutableList;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosValue;

public class EmptyPaxosLearner implements PaxosLearner {

    @Override
    public void learn(long seq, PaxosValue val) {
        // no op
    }

    @Override
    public PaxosValue getLearnedValue(long seq) {
        return null;
    }

    @Override
    public PaxosValue getGreatestLearnedValue() {
        return null;
    }

    @Override
    public Collection<PaxosValue> getLearnedValuesSince(long seq) {
        return ImmutableList.of();
    }

}
