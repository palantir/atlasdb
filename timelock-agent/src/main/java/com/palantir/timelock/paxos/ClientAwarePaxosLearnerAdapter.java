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

import com.palantir.atlasdb.timelock.paxos.Client;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosValue;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class ClientAwarePaxosLearnerAdapter implements PaxosLearner {
    private final String client;
    private final ClientAwarePaxosLearner clientAwarePaxosLearner;

    private ClientAwarePaxosLearnerAdapter(Client client, ClientAwarePaxosLearner clientAwarePaxosLearner) {
        this.client = client.value();
        this.clientAwarePaxosLearner = clientAwarePaxosLearner;
    }

    @Override
    public void learn(long seq, PaxosValue val) {
        clientAwarePaxosLearner.learn(client, seq, val);
    }

    @Nullable
    @Override
    public PaxosValue getLearnedValue(long seq) {
        return clientAwarePaxosLearner.getLearnedValue(client, seq);
    }

    @Nullable
    @Override
    public PaxosValue getGreatestLearnedValue() {
        return clientAwarePaxosLearner.getGreatestLearnedValue(client);
    }

    @Nonnull
    @Override
    public Collection<PaxosValue> getLearnedValuesSince(long seq) {
        return clientAwarePaxosLearner.getLearnedValuesSince(client, seq);
    }

    /**
     * Given a list of {@link ClientAwarePaxosLearner}s, returns a function allowing for injection of the client name.
     */
    public static Function<Client, List<PaxosLearner>> wrap(List<ClientAwarePaxosLearner> learners) {
        return client -> learners.stream()
                .map(learner -> new ClientAwarePaxosLearnerAdapter(client, learner))
                .collect(Collectors.toList());
    }
}
