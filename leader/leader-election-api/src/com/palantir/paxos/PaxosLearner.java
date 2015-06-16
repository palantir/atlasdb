// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.paxos;

import java.util.Collection;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.palantir.common.annotation.Inclusive;

public interface PaxosLearner {

    /**
     * Learn given value for the seq-th round.
     *
     * @param seq round in question
     * @param val value learned for that round
     */
    public void learn(long seq, PaxosValue val);

    /**
     * @return learned value or null if non-exists
     */
    @Nullable
    public PaxosValue getLearnedValue(long seq);

    /**
     * @return the learned value for the greatest known round or null if nothing has been learned
     */
    @Nullable
    public PaxosValue getGreatestLearnedValue();

    /**
     * Returns some collection of learned values since the seq-th round (inclusive)
     *
     * @param seq lower round cutoff for returned values
     * @return some set of learned values for rounds since the seq-th round
     */
    @Nonnull
    Collection<PaxosValue> getLearnedValuesSince(@Inclusive long seq);

}
