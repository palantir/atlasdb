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
