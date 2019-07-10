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

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.SetMultimap;
import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.atlasdb.timelock.paxos.PaxosQuorumCheckingCoalescingFunction.PaxosContainer;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.PaxosValue;

final class LearnedValuesCoalescingFunction implements
        CoalescingRequestFunction<WithSeq<Client>, PaxosContainer<Optional<PaxosValue>>> {

    private final BatchPaxosLearner delegate;

    LearnedValuesCoalescingFunction(BatchPaxosLearner delegate) {
        this.delegate = delegate;
    }

    @Override
    public PaxosContainer<Optional<PaxosValue>> defaultValue() {
        return new PaxosContainer<>(Optional.empty());
    }

    @Override
    public Map<WithSeq<Client>, PaxosContainer<Optional<PaxosValue>>> apply(Set<WithSeq<Client>> request) {
        SetMultimap<Client, PaxosValue> learnedValues = delegate.getLearnedValues(request);
        return KeyedStream.stream(learnedValues)
                .mapKeys((client, paxosValue) -> WithSeq.of(paxosValue.getRound(), client))
                .map(Optional::ofNullable)
                .map(PaxosContainer::new)
                .collectToMap();
    }

}
