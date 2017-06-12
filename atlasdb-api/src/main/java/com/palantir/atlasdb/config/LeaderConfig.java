/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.config;

import java.io.File;
import java.util.Optional;
import java.util.Set;

import javax.validation.constraints.Size;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.palantir.remoting2.config.ssl.SslConfiguration;

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
    public String lockCreator() {
        return leaders().stream()
                .sorted()
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("The leaders block cannot be empty"));
    }

    @Value.Default
    public long pingRateMs() {
        return 5000L;
    }

    @Value.Default
    public long randomWaitBeforeProposingLeadershipMs() {
        return 1000L;
    }

    @Value.Default
    public long leaderPingResponseWaitMs() {
        return 5000L;
    }

    @Value.Check
    protected final void check() {
        Preconditions.checkState(quorumSize() > leaders().size() / 2,
                "The quorumSize '%s' must be over half the amount of leader entries %s.", quorumSize(), leaders());
        Preconditions.checkState(leaders().size() >= quorumSize(),
                "The quorumSize '%s' must be less than or equal to the amount of leader entries %s.",
                quorumSize(), leaders());

        Preconditions.checkArgument(leaders().contains(localServer()),
                "The localServer '%s' must included in the leader entries %s.", localServer(), leaders());
        Preconditions.checkArgument(learnerLogDir().exists() || learnerLogDir().mkdirs(),
                "Learner log directory '%s' does not exist and cannot be created.", learnerLogDir());
        Preconditions.checkArgument(acceptorLogDir().exists() || acceptorLogDir().mkdirs(),
                "Acceptor log directory '%s' does not exist and cannot be created.", acceptorLogDir());
    }

    @JsonIgnore
    @Value.Derived
    public LockLeader whoIsTheLockLeader() {
        return LockLeader.fromBoolean(lockCreator().equals(localServer()));
    }
}
