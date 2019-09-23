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

package com.palantir.timelock.paxos;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.palantir.atlasdb.timelock.paxos.Client;
import com.palantir.atlasdb.timelock.paxos.PaxosUseCase;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosValue;

public final class TimelockPaxosLearnerAdapter implements PaxosLearner {
    private final PaxosUseCase paxosUseCase;
    private final String client;
    private final TimelockPaxosLearnerRpcClient clientAwarePaxosLearner;

    private TimelockPaxosLearnerAdapter(
            PaxosUseCase paxosUseCase,
            Client client,
            TimelockPaxosLearnerRpcClient clientAwarePaxosLearner) {
        this.paxosUseCase = paxosUseCase;
        this.client = client.value();
        this.clientAwarePaxosLearner = clientAwarePaxosLearner;
    }

    @Override
    public void learn(long seq, PaxosValue val) {
        clientAwarePaxosLearner.learn(paxosUseCase, client, seq, val);
    }

    @Nullable
    @Override
    public PaxosValue getLearnedValue(long seq) {
        return clientAwarePaxosLearner.getLearnedValue(paxosUseCase, client, seq);
    }

    @Nullable
    @Override
    public PaxosValue getGreatestLearnedValue() {
        return clientAwarePaxosLearner.getGreatestLearnedValue(paxosUseCase, client);
    }

    @Nonnull
    @Override
    public Collection<PaxosValue> getLearnedValuesSince(long seq) {
        return clientAwarePaxosLearner.getLearnedValuesSince(paxosUseCase, client, seq);
    }

    /**
     * Given a list of {@link TimelockPaxosLearnerRpcClient}s, returns a function allowing for injection of the client name.
     */
    public static Function<Client, List<PaxosLearner>> wrap(
            PaxosUseCase paxosUseCase,
            List<TimelockPaxosLearnerRpcClient> learners) {
        return client -> learners.stream()
                .map(learner -> new TimelockPaxosLearnerAdapter(paxosUseCase, client, learner))
                .collect(Collectors.toList());
    }
}
