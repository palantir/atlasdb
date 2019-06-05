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
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.PaxosPromise;
import com.palantir.paxos.PaxosProposalId;

final class PrepareCoalescingFunction implements
        CoalescingRequestFunction<Map.Entry<Client, WithSeq<PaxosProposalId>>, PaxosPromise> {

    private final BatchPaxosAcceptor delegate;

    PrepareCoalescingFunction(BatchPaxosAcceptor delegate) {
        this.delegate = delegate;
    }

    @Override
    public PaxosPromise defaultValue() {
        throw new AssertionError("no default value");
    }

    @Override
    public Map<Map.Entry<Client, WithSeq<PaxosProposalId>>, PaxosPromise> apply(
            Set<Map.Entry<Client, WithSeq<PaxosProposalId>>> request) {
        SetMultimap<Client, WithSeq<PaxosProposalId>> requests = ImmutableSetMultimap.copyOf(request);

        return KeyedStream.stream(delegate.prepare(requests))
                .mapKeys((client, paxosPromiseWithSeq) -> Maps.immutableEntry(client,
                        WithSeq.of(paxosPromiseWithSeq.seq(), paxosPromiseWithSeq.value().getPromisedId())))
                .map(WithSeq::value)
                .collectToMap();
    }
}
