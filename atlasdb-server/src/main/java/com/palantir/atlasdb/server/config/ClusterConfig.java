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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.remoting.ssl.SslConfiguration;

@JsonSerialize(as = ImmutableClusterConfig.class)
@JsonDeserialize(as = ImmutableClusterConfig.class)
@Value.Immutable
public abstract class ClusterConfig {
    @Value.Default
    public LeaderElectionConfig leaderElection() {
        return LeaderElectionConfig.DEFAULT;
    }

    public abstract String localServer();

    @Size(min = 1)
    public abstract Set<String> servers();

    public abstract Optional<SslConfiguration> security();

    @Value.Check
    protected void check() {
        Preconditions.checkArgument(servers().contains(localServer()),
                "The localServer '%s' must included in the server entries %s.", localServer(), servers());
    }

    @JsonIgnore
    @Value.Derived
    public LeaderConfig toLeaderConfig() {
        Preconditions.checkState(
                leaderElection().consensusAlgorithm() instanceof PaxosConsensusAlgorithmConfig,
                "Only the paxos consensus algorthm is supported");
        PaxosConsensusAlgorithmConfig consensusAlgorithm =
                (PaxosConsensusAlgorithmConfig) leaderElection().consensusAlgorithm();

        return ImmutableLeaderConfig.builder()
                .quorumSize(servers().size() / 2 + 1)
                .localServer(localServer())
                .leaders(servers())
                .sslConfiguration(security())
                .learnerLogDir(consensusAlgorithm.learnerLogDir())
                .acceptorLogDir(consensusAlgorithm.acceptorLogDir())
                .pingRateMs(leaderElection().pingRateMs())
                .randomWaitBeforeProposingLeadershipMs(leaderElection().randomWaitBeforeProposingLeadershipMs())
                .leaderPingResponseWaitMs(leaderElection().leaderPingResponseWaitMs())
                .build();
    }
}
