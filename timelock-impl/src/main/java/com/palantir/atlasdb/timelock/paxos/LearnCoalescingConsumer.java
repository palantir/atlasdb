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
import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.paxos.PaxosResponse;
import com.palantir.paxos.PaxosResponseImpl;
import com.palantir.paxos.PaxosValue;

final class LearnCoalescingConsumer implements CoalescingRequestFunction<Map.Entry<Client, PaxosValue>, PaxosResponse> {
    private static final PaxosResponse SUCCESSFUL_RESPONSE = new PaxosResponseImpl(true);

    private final BatchPaxosLearner delegate;

    LearnCoalescingConsumer(BatchPaxosLearner delegate) {
        this.delegate = delegate;
    }

    @Override
    public PaxosResponse defaultValue() {
        return new PaxosResponseImpl(false);
    }

    @Override
    public Map<Map.Entry<Client, PaxosValue>, PaxosResponse> apply(Set<Map.Entry<Client, PaxosValue>> request) {
        delegate.learn(ImmutableSetMultimap.copyOf(request));
        return Maps.toMap(request, $ -> SUCCESSFUL_RESPONSE);
    }
}
