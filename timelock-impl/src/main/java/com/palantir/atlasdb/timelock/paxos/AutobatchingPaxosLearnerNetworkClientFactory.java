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

import static com.palantir.atlasdb.timelock.paxos.PaxosQuorumCheckingCoalescingFunction.wrap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.timelock.paxos.PaxosQuorumCheckingCoalescingFunction.PaxosContainer;
import com.palantir.common.concurrent.CheckedRejectionExecutorService;
import com.palantir.logsafe.Preconditions;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosLearnerNetworkClient;
import com.palantir.paxos.PaxosResponse;
import com.palantir.paxos.PaxosResponses;
import com.palantir.paxos.PaxosUpdate;
import com.palantir.paxos.PaxosValue;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

public final class AutobatchingPaxosLearnerNetworkClientFactory implements Closeable {

    private final DisruptorAutobatcher<Map.Entry<Client, PaxosValue>, PaxosResponse> learn;
    private final DisruptorAutobatcher<WithSeq<Client>, PaxosResponses<PaxosContainer<Optional<PaxosValue>>>>
            getLearnedValues;
    private final DisruptorAutobatcher<WithSeq<Client>, PaxosResponses<PaxosUpdate>> getLearnedValuesSince;

    private AutobatchingPaxosLearnerNetworkClientFactory(
            DisruptorAutobatcher<Map.Entry<Client, PaxosValue>, PaxosResponse> learn,
            DisruptorAutobatcher<WithSeq<Client>, PaxosResponses<PaxosContainer<Optional<PaxosValue>>>>
                    getLearnedValues,
            DisruptorAutobatcher<WithSeq<Client>, PaxosResponses<PaxosUpdate>> getLearnedValuesSince) {
        this.learn = learn;
        this.getLearnedValues = getLearnedValues;
        this.getLearnedValuesSince = getLearnedValuesSince;
    }

    public static AutobatchingPaxosLearnerNetworkClientFactory createForTests(
            LocalAndRemotes<BatchPaxosLearner> learners, ExecutorService executorService, int quorumSize) {
        return create(
                learners.map(batchPaxosLearner -> WithDedicatedExecutor.of(batchPaxosLearner, executorService)),
                quorumSize);
    }

    public static AutobatchingPaxosLearnerNetworkClientFactory create(
            LocalAndRemotes<WithDedicatedExecutor<BatchPaxosLearner>> learners, int quorumSize) {
        DisruptorAutobatcher<Map.Entry<Client, PaxosValue>, PaxosResponse> learn = Autobatchers.coalescing(
                        new LearnCoalescingConsumer(learners.local(), learners.remotes()))
                .safeLoggablePurpose("batch-paxos-learner.learn")
                .build();

        Map<BatchPaxosLearner, CheckedRejectionExecutorService> executors =
                WithDedicatedExecutor.convert(learners.all());
        List<BatchPaxosLearner> remotes = ImmutableList.copyOf(executors.keySet());

        DisruptorAutobatcher<WithSeq<Client>, PaxosResponses<PaxosContainer<Optional<PaxosValue>>>> learnedValues =
                Autobatchers.coalescing(wrap(remotes, executors, quorumSize, LearnedValuesCoalescingFunction::new))
                        .safeLoggablePurpose("batch-paxos-learner.learned-values")
                        .build();

        DisruptorAutobatcher<WithSeq<Client>, PaxosResponses<PaxosUpdate>> learnedValuesSince = Autobatchers.coalescing(
                        wrap(remotes, executors, quorumSize, LearnedValuesSinceCoalescingFunction::new))
                .safeLoggablePurpose("batch-paxos-learner.learned-values-since")
                .build();

        return new AutobatchingPaxosLearnerNetworkClientFactory(learn, learnedValues, learnedValuesSince);
    }

    public PaxosLearnerNetworkClient paxosLearnerForClient(Client client) {
        return new AutobatchingPaxosLearnerNetworkClient(client);
    }

    @Override
    public void close() {
        learn.close();
        getLearnedValues.close();
        getLearnedValuesSince.close();
    }

    private final class AutobatchingPaxosLearnerNetworkClient implements PaxosLearnerNetworkClient {

        private final Client client;

        private AutobatchingPaxosLearnerNetworkClient(Client client) {
            this.client = client;
        }

        @Override
        public void learn(long seq, PaxosValue value) {
            Preconditions.checkArgument(seq == value.getRound(), "seq differs from PaxosValue.round");
            try {
                learn.apply(Maps.immutableEntry(client, value)).get();
            } catch (ExecutionException | InterruptedException e) {
                throw AutobatcherExecutionExceptions.handleAutobatcherExceptions(e);
            }
        }

        @Override
        public <T extends PaxosResponse> PaxosResponses<T> getLearnedValue(
                long seq, Function<Optional<PaxosValue>, T> mapper) {
            try {
                return getLearnedValues.apply(WithSeq.of(client, seq)).get().map(c -> mapper.apply(c.get()));
            } catch (ExecutionException | InterruptedException e) {
                throw AutobatcherExecutionExceptions.handleAutobatcherExceptions(e);
            }
        }

        @Override
        public PaxosResponses<PaxosUpdate> getLearnedValuesSince(long seq) {
            try {
                return getLearnedValuesSince.apply(WithSeq.of(client, seq)).get();
            } catch (ExecutionException | InterruptedException e) {
                throw AutobatcherExecutionExceptions.handleAutobatcherExceptions(e);
            }
        }
    }
}
