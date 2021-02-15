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

import com.palantir.common.streams.KeyedStream;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosValue;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class BatchPingableLeaderResource implements BatchPingableLeader {

    private final UUID leadershipUuid;
    private final LocalPaxosComponents components;

    public BatchPingableLeaderResource(UUID leadershipUuid, LocalPaxosComponents components) {
        this.leadershipUuid = leadershipUuid;
        this.components = components;
    }

    @Override
    public Set<Client> ping(Set<Client> clients) {
        return KeyedStream.of(clients)
                .map(components::learner)
                .map(PaxosLearner::getGreatestLearnedValue)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(this::isThisNodeTheLeaderFor)
                .keys()
                .collect(Collectors.toSet());
    }

    private boolean isThisNodeTheLeaderFor(PaxosValue value) {
        return value.getLeaderUUID().equals(leadershipUuid.toString());
    }

    @Override
    public UUID uuid() {
        return leadershipUuid;
    }
}
