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

package com.palantir.paxos;

import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;
import com.palantir.common.concurrent.CheckedRejectedExecutionException;
import com.palantir.common.concurrent.CheckedRejectionExecutorService;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

public class SingleLeaderLearnerNetworkClient implements PaxosLearnerNetworkClient {

    private static final SafeLogger log = SafeLoggerFactory.get(SingleLeaderLearnerNetworkClient.class);

    private final PaxosLearner localLearner;
    private final ImmutableList<PaxosLearner> remoteLearners;
    private final ImmutableList<PaxosLearner> allLearners;
    private final int quorumSize;
    private final Map<PaxosLearner, CheckedRejectionExecutorService> executors;
    private final boolean cancelRemainingCalls;

    public SingleLeaderLearnerNetworkClient(
            PaxosLearner localLearner,
            List<PaxosLearner> remoteLearners,
            int quorumSize,
            Map<PaxosLearner, CheckedRejectionExecutorService> executors,
            boolean cancelRemainingCalls) {
        this.localLearner = localLearner;
        this.remoteLearners = ImmutableList.copyOf(remoteLearners);
        this.quorumSize = quorumSize;
        this.executors = executors;
        this.cancelRemainingCalls = cancelRemainingCalls;
        this.allLearners = ImmutableList.<PaxosLearner>builder()
                .add(localLearner)
                .addAll(remoteLearners)
                .build();
    }

    public static SingleLeaderLearnerNetworkClient createLegacy(
            PaxosLearner localLearner,
            List<PaxosLearner> remoteLearners,
            int quorumSize,
            Map<PaxosLearner, ExecutorService> executors,
            boolean cancelRemainingCalls) {
        return new SingleLeaderLearnerNetworkClient(
                localLearner,
                remoteLearners,
                quorumSize,
                KeyedStream.stream(executors)
                        .map(CheckedRejectionExecutorService::new)
                        .collectToMap(),
                cancelRemainingCalls);
    }

    @Override
    public void learn(long seq, PaxosValue value) {
        // broadcast learned value
        for (final PaxosLearner learner : remoteLearners) {
            try {
                executors.get(learner).execute(() -> {
                    try {
                        learner.learn(seq, value);
                    } catch (Throwable e) {
                        log.warn(
                                "Failed to teach learner the value {} at sequence {}, after attempting execution.",
                                UnsafeArg.of("value", base16EncodePaxosValue(value)),
                                SafeArg.of("sequence", seq),
                                e);
                    }
                });
            } catch (CheckedRejectedExecutionException e) {
                log.warn(
                        "Failed to teach learner the value {} at sequence {}, because we could not execute the task.",
                        UnsafeArg.of("value", base16EncodePaxosValue(value)),
                        SafeArg.of("sequence", seq),
                        e);
            }
        }

        // force local learner to update
        localLearner.learn(seq, value);
    }

    @Override
    public <T extends PaxosResponse> PaxosResponses<T> getLearnedValue(
            long seq, Function<Optional<PaxosValue>, T> mapper) {
        return PaxosQuorumChecker.collectQuorumResponses(
                        allLearners,
                        learner -> mapper.apply(learner.getLearnedValue(seq)),
                        quorumSize,
                        executors,
                        PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT,
                        cancelRemainingCalls)
                .withoutRemotes();
    }

    @Override
    public PaxosResponses<PaxosUpdate> getLearnedValuesSince(long seq) {
        return PaxosQuorumChecker.collectQuorumResponses(
                        allLearners,
                        learner -> new PaxosUpdate(ImmutableList.copyOf(learner.getLearnedValuesSince(seq))),
                        quorumSize,
                        executors,
                        PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT,
                        cancelRemainingCalls)
                .withoutRemotes();
    }

    private static String base16EncodePaxosValue(PaxosValue value) {
        return Optional.ofNullable(value.data)
                .map(bytes -> BaseEncoding.base16().encode(bytes))
                .orElse(null);
    }
}
