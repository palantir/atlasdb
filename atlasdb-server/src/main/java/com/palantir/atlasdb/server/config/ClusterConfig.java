/**
 * Copyright 2016 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.server.config;

import java.util.Set;

import javax.validation.constraints.Size;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.config.LeaderConfig;

@JsonSerialize(as = ImmutableClusterConfig.class)
@JsonDeserialize(as = ImmutableClusterConfig.class)
@Value.Immutable
public abstract class ClusterConfig {
    @Value.Default
    public LeaderElectionConfig leaderElection() {
        return LeaderElectionConfig.DEFAULT;
    }

    public abstract int quorumSize();

    public abstract String localServer();

    @Size(min = 1)
    public abstract Set<String> servers();

    @JsonProperty("lockCreator")
    @Value.Default
    public String lockCreator() {
        return servers().stream()
                .sorted()
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No servers have been specified."));
    }

    @Value.Check
    protected void check() {
        Preconditions.checkState(quorumSize() > servers().size() / 2,
                "The quorumSize '%s' must be over half the amount of server entries %s.", quorumSize(), servers());
        Preconditions.checkState(servers().size() >= quorumSize(),
                "The quorumSize '%s' must be less than or equal to the amount of server entries %s.",
                quorumSize(), servers());
        Preconditions.checkArgument(servers().contains(localServer()),
                "The localServer '%s' must included in the server entries %s.", localServer(), servers());
    }

    @JsonIgnore
    @Value.Derived
    public LeaderConfig toLeaderConfig() {
        return ImmutableLeaderConfig.builder()
                .quorumSize(quorumSize())
                .localServer(localServer())
                .leaders(servers())
                .lockCreator(lockCreator())
                .learnerLogDir(leaderElection().paxos().learnerLogDir())
                .acceptorLogDir(leaderElection().paxos().acceptorLogDir())
                .pingRateMs(leaderElection().pingRateMs())
                .randomWaitBeforeProposingLeadershipMs(leaderElection().randomWaitBeforeProposingLeadershipMs())
                .leaderPingResponseWaitMs(leaderElection().leaderPingResponseWaitMs())
                .build();
    }
}
