/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nullable;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.SafeArg;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosQuorumChecker;
import com.palantir.paxos.PaxosResponse;
import com.palantir.paxos.PaxosValue;

public final class PaxosSynchronizer {
    private static final Logger log = LoggerFactory.getLogger(PaxosSynchronizer.class);

    private PaxosSynchronizer() {
        // utility
    }

    public static void synchronizeLearner(PaxosLearner learnerToSynchronize,
                                          List<PaxosLearner> paxosLearners) {
        Optional<PaxosValue> mostRecentValue = getMostRecentLearnedValue(paxosLearners);
        if (mostRecentValue.isPresent()) {
            PaxosValue paxosValue = mostRecentValue.get();
            if (paxosValue.equals(learnerToSynchronize.getGreatestLearnedValue())) {
                log.info("Started up and found that our value {} is already the most recent.",
                        SafeArg.of("value", paxosValue));
            } else {
                learnerToSynchronize.learn(paxosValue.getRound(), paxosValue);
                log.info("Started up and learned the most recent value: {}.",
                        SafeArg.of("value", paxosValue));
            }
        } else {
            log.info("Started up, and no one I talked to knows anything yet.");
        }
    }

    private static Optional<PaxosValue> getMostRecentLearnedValue(List<PaxosLearner> paxosLearners) {
        ExecutorService executor = PTExecutors.newCachedThreadPool();
        List<PaxosValueResponse> responses = PaxosQuorumChecker.collectAsManyResponsesAsPossible(
                ImmutableList.copyOf(paxosLearners),
                learner -> ImmutablePaxosValueResponse.of(learner.getGreatestLearnedValue()),
                executor,
                PaxosQuorumChecker.DEFAULT_REMOTE_REQUESTS_TIMEOUT_IN_SECONDS);
        return responses.stream()
                .filter(response -> response.paxosValue() != null)
                .map(PaxosValueResponse::paxosValue)
                .max(Comparator.comparingLong(PaxosValue::getRound));
    }

    @Value.Immutable
    interface PaxosValueResponse extends PaxosResponse {
        default boolean isSuccessful() {
            return true;
        }

        @Value.Parameter
        @Nullable
        PaxosValue paxosValue();
    }
}
