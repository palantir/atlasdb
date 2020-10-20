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

import static com.google.common.collect.ImmutableSortedMap.toImmutableSortedMap;
import static java.util.stream.Collectors.toMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.SetMultimap;
import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosUpdate;
import com.palantir.paxos.PaxosValue;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

final class LearnedValuesSinceCoalescingFunction implements CoalescingRequestFunction<WithSeq<Client>, PaxosUpdate> {

    private final BatchPaxosLearner delegate;

    LearnedValuesSinceCoalescingFunction(BatchPaxosLearner delegate) {
        this.delegate = delegate;
    }

    @Override
    public Map<WithSeq<Client>, PaxosUpdate> apply(Set<WithSeq<Client>> request) {
        Map<Client, Long> remoteRequest = request.stream().collect(toMap(WithSeq::value, WithSeq::seq, Math::min));

        SetMultimap<Client, PaxosValue> learnedValuesSince = delegate.getLearnedValuesSince(remoteRequest);

        Map<Client, ImmutableSortedMap<Long, PaxosValue>> results = KeyedStream.stream(learnedValuesSince.asMap())
                .map(LearnedValuesSinceCoalescingFunction::toNavigableMapBySeq)
                .collectToMap();

        return KeyedStream.of(request)
                .map(clientWithSeq -> results.getOrDefault(clientWithSeq.value(), ImmutableSortedMap.of()))
                .map(LearnedValuesSinceCoalescingFunction::getPaxosValuesSinceSeq)
                .map(values -> new PaxosUpdate(ImmutableList.copyOf(values)))
                .collectToMap();
    }

    private static Collection<PaxosValue> getPaxosValuesSinceSeq(
            WithSeq<Client> clientWithSeq, ImmutableSortedMap<Long, PaxosValue> paxosValuesByRound) {
        return paxosValuesByRound.tailMap(clientWithSeq.seq()).values();
    }

    private static ImmutableSortedMap<Long, PaxosValue> toNavigableMapBySeq(Collection<PaxosValue> values) {
        return KeyedStream.of(values)
                .mapKeys(PaxosValue::getRound)
                .entries()
                .collect(toImmutableSortedMap(Comparator.naturalOrder(), Map.Entry::getKey, Map.Entry::getValue));
    }
}
