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
package com.palantir.atlasdb.timelock.config;

import java.io.File;
import java.util.Optional;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.palantir.remoting.api.config.ssl.SslConfiguration;

@JsonSerialize(as = ImmutablePaxosConfiguration.class)
@JsonDeserialize(as = ImmutablePaxosConfiguration.class)
@Value.Immutable
public abstract class PaxosConfiguration implements TimeLockAlgorithmConfiguration {
    public static final PaxosConfiguration DEFAULT = ImmutablePaxosConfiguration.builder().build();

    @Value.Default
    public File paxosDataDir() {
        return new File("var/data/paxos");
    }

    public abstract Optional<SslConfiguration> sslConfiguration();

    @Value.Default
    public long pingRateMs() {
        return 5000L;
    }

    @Value.Default
    public long maximumWaitBeforeProposalMs() {
        return 1000L;
    }

    @Value.Default
    public long leaderPingResponseWaitMs() {
        return 5000L;
    }

    @Value.Default
    public boolean isNewService() {
        return false;
    }

    @Value.Check
    protected final void check() {
        Preconditions.checkArgument(paxosDataDir().exists() || paxosDataDir().mkdirs(),
                "Paxos data directory '%s' does not exist and cannot be created.", paxosDataDir());
        Preconditions.checkArgument(pingRateMs() > 0,
                "Ping rate must be positive; found '%s'.", pingRateMs());
        Preconditions.checkArgument(maximumWaitBeforeProposalMs() > 0,
                "Maximum wait before proposal must be positive; found '%s'.", maximumWaitBeforeProposalMs());
        Preconditions.checkArgument(leaderPingResponseWaitMs() > 0,
                "Leader ping response wait interval must be positive; found '%s'.", leaderPingResponseWaitMs());
    }
}
