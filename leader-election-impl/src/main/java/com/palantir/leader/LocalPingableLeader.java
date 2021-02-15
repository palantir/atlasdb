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

package com.palantir.leader;

import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosValue;
import com.palantir.sls.versions.OrderableSlsVersion;
import java.util.Optional;
import java.util.UUID;

public final class LocalPingableLeader implements PingableLeader {
    private final PaxosLearner knowledge;
    private final String localUuid;
    private final Optional<OrderableSlsVersion> timeLockVersion;

    public LocalPingableLeader(PaxosLearner knowledge, UUID localUuid) {
        this.knowledge = knowledge;
        this.localUuid = localUuid.toString();
        this.timeLockVersion = Optional.empty();
    }

    public LocalPingableLeader(PaxosLearner knowledge, UUID localUuid, OrderableSlsVersion timeLockVersion) {
        this.knowledge = knowledge;
        this.localUuid = localUuid.toString();
        this.timeLockVersion = Optional.of(timeLockVersion);
    }

    @Override
    public boolean ping() {
        return knowledge
                .getGreatestLearnedValue()
                .map(this::isThisNodeTheLeaderFor)
                .orElse(false);
    }

    @Override
    public String getUUID() {
        return localUuid;
    }

    @Override
    public PingResult pingV2() {
        return PingResult.builder()
                .isLeader(ping())
                .timeLockVersion(timeLockVersion)
                .build();
    }

    private boolean isThisNodeTheLeaderFor(PaxosValue value) {
        return value.getLeaderUUID().equals(localUuid);
    }
}
