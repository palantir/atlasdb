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
package com.palantir.atlasdb.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import java.io.File;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import javax.validation.constraints.Size;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableLeaderConfig.class)
@JsonSerialize(as = ImmutableLeaderConfig.class)
@Value.Immutable
@SuppressWarnings("DesignForExtension")
public abstract class LeaderConfig {

    public abstract int quorumSize();

    @Value.Default
    public File learnerLogDir() {
        return new File("var/data/paxos/learner");
    }

    @Value.Default
    public File acceptorLogDir() {
        return new File("var/data/paxos/acceptor");
    }

    public abstract String localServer();

    @Size(min = 1)
    public abstract Set<String> leaders();

    public abstract Optional<SslConfiguration> sslConfiguration();

    @Value.Default
    long pingRateMs() {
        return 5000L;
    }

    @JsonIgnore
    @Value.Derived
    @Value.Auxiliary
    public Duration pingRate() {
        return Duration.ofMillis(pingRateMs());
    }

    @Value.Default
    long randomWaitBeforeProposingLeadershipMs() {
        return 1000L;
    }

    @JsonIgnore
    @Value.Derived
    @Value.Auxiliary
    public Duration randomWaitBeforeProposingLeadership() {
        return Duration.ofMillis(randomWaitBeforeProposingLeadershipMs());
    }

    @Value.Default
    long leaderPingResponseWaitMs() {
        return 5000L;
    }

    @JsonIgnore
    @Value.Derived
    @Value.Auxiliary
    public Duration leaderPingResponseWait() {
        return Duration.ofMillis(leaderPingResponseWaitMs());
    }

    @JsonIgnore
    @Value.Default
    public Duration leaderAddressCacheTtl() {
        return Duration.ofSeconds(15);
    }

    @Value.Check
    protected final void check() {
        Preconditions.checkState(
                quorumSize() > leaders().size() / 2,
                "The quorumSize '%s' must be over half the amount of leader entries %s.",
                quorumSize(),
                leaders());
        Preconditions.checkState(
                leaders().size() >= quorumSize(),
                "The quorumSize '%s' must be less than or equal to the amount of leader entries %s.",
                quorumSize(),
                leaders());

        Preconditions.checkArgument(
                leaders().contains(localServer()),
                "The localServer '%s' must included in the leader entries %s.",
                localServer(),
                leaders());
    }
}
