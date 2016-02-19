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

import java.util.ArrayList;
import java.util.Collection;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosValue;

public class InMemoryPaxosLearner implements PaxosLearner {
    final SortedMap<Long, PaxosValue> state = new ConcurrentSkipListMap<Long, PaxosValue>();

    @Override
    public void learn(long seq, PaxosValue val) {
        state.put(seq, val);
    }

    @Override
    public PaxosValue getLearnedValue(long seq) {
        return state.get(seq);
    }

    @Override
    public Collection<PaxosValue> getLearnedValuesSince(long seq) {
        PaxosValue greatestLearnedValue = getGreatestLearnedValue();
        long greatestSeq = -1L;
        if (greatestLearnedValue != null) {
            greatestSeq = greatestLearnedValue.getRound();
        }

        Collection<PaxosValue> values = new ArrayList<PaxosValue>();
        for (long i = seq; i <= greatestSeq; i++) {
            PaxosValue value;
            value = getLearnedValue(i);
            if (value != null) {
                values.add(value);
            }
        }
        return values;
    }

    @Override
    public PaxosValue getGreatestLearnedValue() {
        if (!state.isEmpty()) {
            return state.get(state.lastKey());
        }
        return null;
    }

}
