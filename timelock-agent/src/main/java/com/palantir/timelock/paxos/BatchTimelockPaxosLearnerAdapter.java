/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.timelock.paxos.BatchPaxosLearnerRpcClient;
import com.palantir.atlasdb.timelock.paxos.PaxosUseCase;
import com.palantir.atlasdb.timelock.paxos.WithSeq;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosValue;
import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class BatchTimelockPaxosLearnerAdapter implements PaxosLearner {
    private final PaxosUseCase paxosUseCase;
    private final Client client;
    private final BatchPaxosLearnerRpcClient rpcClient;

    private AtomicLong lastKnownSequence = new AtomicLong(Long.MIN_VALUE);

    public BatchTimelockPaxosLearnerAdapter(
            PaxosUseCase paxosUseCase,
            Client client,
            BatchPaxosLearnerRpcClient rpcClient) {
        this.paxosUseCase = paxosUseCase;
        this.client = client;
        this.rpcClient = rpcClient;
    }

    public static PaxosLearner singleLeader(BatchPaxosLearnerRpcClient rpcClient) {
        return new BatchTimelockPaxosLearnerAdapter(
                PaxosUseCase.LEADER_FOR_ALL_CLIENTS,
                PaxosUseCase.PSEUDO_LEADERSHIP_CLIENT,
                rpcClient);
    }

    @Override
    public void learn(long seq, PaxosValue val) {
        rpcClient.learn(paxosUseCase, ImmutableSetMultimap.of(client, val));
        updateLastKnownSequence(seq);
    }

    @Override
    public Optional<PaxosValue> getLearnedValue(long seq) {
        Set<PaxosValue> result = rpcClient.getLearnedValues(paxosUseCase, ImmutableSet.of(WithSeq.of(client, seq)))
                .get(client);
        checkResult(result);
        if (result.isEmpty()) {
            return Optional.empty();
        }
        updateLastKnownSequence(seq);
        return Optional.of(Iterables.getOnlyElement(result));
    }

    @Override
    public Optional<PaxosValue> getGreatestLearnedValue() {
        Set<PaxosValue> result = rpcClient
                .getLearnedValuesSince(paxosUseCase, ImmutableMap.of(client, lastKnownSequence.get()))
                .get(client);
        Optional<PaxosValue> greatestIfExists = result.stream().max(Comparator.comparingLong(PaxosValue::getRound));
        greatestIfExists.ifPresent(paxosValue -> updateLastKnownSequence(paxosValue.getRound()));
        return greatestIfExists;
    }

    @Override
    public Collection<PaxosValue> getLearnedValuesSince(long seq) {
        Set<PaxosValue> result = rpcClient.getLearnedValuesSince(paxosUseCase, ImmutableMap.of(client, seq))
                .get(client);
        result.stream().map(PaxosValue::getRound).mapToLong(x -> x).max().ifPresent(this::updateLastKnownSequence);
        return result;
    }

    private <T> void checkResult(Set<T> result) {
        Preconditions.checkState(result.size() <= 1,
                "Unexpected result {} in a call for client {}.",
                SafeArg.of("result", result),
                SafeArg.of("client", client));
    }

    private void updateLastKnownSequence(long seq) {
        lastKnownSequence.accumulateAndGet(seq, Long::max);
    }
}
