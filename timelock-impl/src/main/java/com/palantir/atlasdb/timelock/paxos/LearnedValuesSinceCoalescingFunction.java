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

import static java.util.stream.Collectors.toMap;

import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.SetMultimap;
import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.PaxosValue;

final class LearnedValuesSinceCoalescingFunction
        implements CoalescingRequestFunction<WithSeq<Client>, Collection<PaxosValue>> {

    private final BatchPaxosLearner delegate;

    LearnedValuesSinceCoalescingFunction(BatchPaxosLearner delegate) {
        this.delegate = delegate;
    }

    @Override
    public Collection<PaxosValue> defaultValue() {
        return ImmutableSet.of();
    }

    @Override
    public Map<WithSeq<Client>, Collection<PaxosValue>> apply(Set<WithSeq<Client>> request) {
        Map<Client, Long> remoteRequest = request.stream()
                .collect(toMap(WithSeq::value, WithSeq::seq, Math::min));

        SetMultimap<Client, PaxosValue> learnedValuesSince = delegate.getLearnedValuesSince(remoteRequest);

        Map<Client, NavigableMap<Long, PaxosValue>> results = KeyedStream.stream(learnedValuesSince.asMap())
                .map(LearnedValuesSinceCoalescingFunction::toTreeMapBySeq)
                .collectToMap();

        return KeyedStream.of(request)
                .map(clientWithSeq -> results.getOrDefault(clientWithSeq.value(), ImmutableSortedMap.of()))
                .map((clientWithSeq, paxosValuesByRound) -> paxosValuesByRound.tailMap(clientWithSeq.seq()).values())
                .collectToMap();
    }

    private static NavigableMap<Long, PaxosValue> toTreeMapBySeq(Collection<PaxosValue> values) {
        return KeyedStream.of(values)
                .mapKeys(PaxosValue::getRound)
                .<NavigableMap<Long, PaxosValue>>collectTo(TreeMap::new);
    }
}
