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

import com.palantir.atlasdb.timelock.paxos.PaxosRemoteClients;
import com.palantir.atlasdb.timelock.paxos.PaxosUseCase;
import com.palantir.atlasdb.timelock.paxos.WithDedicatedExecutor;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosLearner;
import java.util.List;
import java.util.stream.Collectors;

public final class TimelockPaxosLearnerAdapters {
    private TimelockPaxosLearnerAdapters() {
        // how many times
    }

    public static List<WithDedicatedExecutor<PaxosLearner>> create(
            PaxosUseCase paxosUseCase, PaxosRemoteClients remoteClients, Client client) {
        return switch (paxosUseCase) {
            case LEADER_FOR_ALL_CLIENTS -> remoteClients.singleLeaderLearner();
            case TIMESTAMP -> remoteClients.timestampLearner().stream()
                    .map(withDedicatedExecutor -> withDedicatedExecutor.<PaxosLearner>transformService(
                            learner -> new TimelockPaxosLearnerAdapter(paxosUseCase, client.value(), learner)))
                    .collect(Collectors.toList());
        };
    }
}
