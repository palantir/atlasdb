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
import java.util.Set;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.PaxosProposal;

final class AcceptCoalescingFunction implements
        CoalescingRequestFunction<Map.Entry<Client, PaxosProposal>, BooleanPaxosResponse> {

    private final BatchPaxosAcceptor delegate;

    AcceptCoalescingFunction(BatchPaxosAcceptor delegate) {
        this.delegate = delegate;
    }

    @Override
    public BooleanPaxosResponse defaultValue() {
        return new BooleanPaxosResponse(false);
    }

    @Override
    public Map<Map.Entry<Client, PaxosProposal>, BooleanPaxosResponse> apply(
            Set<Map.Entry<Client, PaxosProposal>> request) {
        SetMultimap<Client, PaxosProposal> requests = ImmutableSetMultimap.copyOf(request);
        Map<WithSeq<Client>, BooleanPaxosResponse> results = KeyedStream.stream(delegate.accept(requests))
                .mapKeys((client, booleanResponseWithSeq) -> WithSeq.of(booleanResponseWithSeq.seq(), client))
                .map(WithSeq::value)
                .collectToMap();

        return KeyedStream.of(request)
                .map(clientAndProposal -> results.get(
                        WithSeq.of(clientAndProposal.getValue().getValue().getRound(), clientAndProposal.getKey())))
                .collectToMap();
    }

}
