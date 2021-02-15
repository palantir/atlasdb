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

import com.google.common.collect.SetMultimap;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosValue;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class LocalBatchPaxosLearner implements BatchPaxosLearner {

    private final LocalPaxosComponents paxosComponents;

    public LocalBatchPaxosLearner(LocalPaxosComponents paxosComponents) {
        this.paxosComponents = paxosComponents;
    }

    @Override
    public void learn(SetMultimap<Client, PaxosValue> paxosValuesByClient) {
        paxosValuesByClient.forEach(
                (client, paxosValue) -> paxosComponents.learner(client).learn(paxosValue.getRound(), paxosValue));
    }

    @Override
    public SetMultimap<Client, PaxosValue> getLearnedValues(Set<WithSeq<Client>> clientAndSeqs) {
        return KeyedStream.of(clientAndSeqs)
                .mapKeys(WithSeq::value)
                .map(WithSeq::seq)
                .map((client, seq) -> paxosComponents.learner(client).getLearnedValue(seq))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collectToSetMultimap();
    }

    @Override
    public SetMultimap<Client, PaxosValue> getLearnedValuesSince(Map<Client, Long> seqLowerBoundsByClient) {
        return KeyedStream.stream(seqLowerBoundsByClient)
                .map((client, seq) -> paxosComponents.learner(client).getLearnedValuesSince(seq))
                .flatMap(Collection::stream)
                .collectToSetMultimap();
    }
}
