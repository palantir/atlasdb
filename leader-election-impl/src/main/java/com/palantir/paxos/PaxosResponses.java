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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class PaxosResponses<T extends PaxosResponse> {
    private final int totalRequests;
    private final int quorum;
    private final boolean shortcut;
    private List<T> responses = new ArrayList<>();
    private int successes = 0;
    private int failures = 0;

    PaxosResponses(int totalRequests, int quorum, boolean shortcut) {
        this.totalRequests = totalRequests;
        this.quorum = quorum;
        this.shortcut = shortcut;
    }

    public boolean hasMoreRequests() {
        return successes + failures < totalRequests;
    }

    boolean shouldProcessNextRequest() {
        return !hasQuorum() && !shouldGiveUpOnAchievingQuorum();
    }

    public void add(T response) {
        if (response.isSuccessful()) {
            successes++;
        } else {
            failures++;
        }
        responses.add(response);
    }

    void markFailure() {
        failures++;
    }

    public List<T> get() {
        return responses;
    }

    public Stream<T> stream() {
        return responses.stream();
    }

    public boolean hasQuorum() {
        return successes >= quorum;
    }

    PaxosQuorumStatus getQuorumResult() {
        if (hasQuorum()) {
            return PaxosQuorumStatus.QUORUM_AGREED;
        } else if (thereWereDisagreements()) {
            return PaxosQuorumStatus.SOME_DISAGREED;
        }
        return PaxosQuorumStatus.NO_QUORUM;
    }

    private boolean shouldGiveUpOnAchievingQuorum() {
        return shortcut && failures > totalRequests - quorum;
    }

    private boolean thereWereDisagreements() {
        return responses.stream().anyMatch(response -> !response.isSuccessful());
    }
}
