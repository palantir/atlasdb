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

import java.util.LinkedList;
import java.util.List;

final class PaxosResponseAccumulator<T extends PaxosResponse> {

    private final int totalRequests;
    private final int quorum;
    private final boolean shortcut;

    private int successes = 0;
    private int failures = 0;
    private List<T> responses = new LinkedList<>();

    private PaxosResponseAccumulator(int totalRequests, int quorum, boolean shortcut) {
        this.totalRequests = totalRequests;
        this.quorum = quorum;
        this.shortcut = shortcut;
    }

    static <T extends PaxosResponse> PaxosResponseAccumulator<T> newResponse(
            int totalRequests,
            int quorumSize,
            boolean shortcut) {
        return new PaxosResponseAccumulator<>(totalRequests, quorumSize, shortcut);
    }

    void add(T response) {
        if (response.isSuccessful()) {
            successes++;
        } else {
            failures++;
        }
        responses.add(response);
    }

    boolean hasMoreRequests() {
        return successes + failures < totalRequests;
    }

    private boolean shouldGiveUpOnAchievingQuorum() {
        return shortcut && failures > totalRequests - quorum;
    }

    boolean hasQuorum() {
        return successes >= quorum;
    }

    boolean shouldProcessNextRequest() {
        return !hasQuorum() && !shouldGiveUpOnAchievingQuorum();
    }

    void markFailure() {
        failures++;
    }

    PaxosResponses<T> collect() {
        return PaxosResponses.of(quorum, responses);
    }

}
