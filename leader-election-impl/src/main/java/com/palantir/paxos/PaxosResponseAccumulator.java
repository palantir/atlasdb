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

import com.palantir.logsafe.Preconditions;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Predicate;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
final class PaxosResponseAccumulator<S, T extends PaxosResponse> {

    private final int totalRequests;
    private final int quorum;
    private final Predicate<InProgressResponseState<S, T>> shouldShortcutPredicate;
    private final Map<S, T> responsesByService;

    private int successes = 0;
    private int failures = 0;

    private PaxosResponseAccumulator(
            int totalRequests, int quorum, Predicate<InProgressResponseState<S, T>> shouldShortcutPredicate) {
        this.totalRequests = totalRequests;
        this.quorum = quorum;
        this.shouldShortcutPredicate = shouldShortcutPredicate;
        this.responsesByService = new LinkedHashMap<>(totalRequests);
    }

    static <S, T extends PaxosResponse> PaxosResponseAccumulator<S, T> newResponse(
            int totalRequests, int quorumSize, Predicate<InProgressResponseState<S, T>> customShortcutPredicate) {
        return new PaxosResponseAccumulator<>(totalRequests, quorumSize, customShortcutPredicate);
    }

    void add(S service, T response) {
        if (response.isSuccessful()) {
            successes++;
        } else {
            failures++;
        }
        T oldValue = responsesByService.put(service, response);
        Preconditions.checkState(oldValue == null, "Received response for same service multiple times!");
    }

    boolean hasMoreRequests() {
        return successes + failures < totalRequests;
    }

    boolean hasQuorum() {
        return successes >= quorum;
    }

    boolean shouldProcessNextRequest() {
        return !shouldShortcutPredicate.test(currentState());
    }

    void markFailure() {
        failures++;
    }

    PaxosResponsesWithRemote<S, T> collect() {
        return PaxosResponsesWithRemote.of(quorum, responsesByService);
    }

    private InProgressResponseState<S, T> currentState() {
        return ImmutableInProgressResponseState.<S, T>builder()
                .responses(responsesByService)
                .successes(successes)
                .failures(failures)
                .totalRequests(totalRequests)
                .build();
    }
}
