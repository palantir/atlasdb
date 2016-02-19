/**
 * Copyright 2015 Palantir Technologies
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
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.Size;

import org.immutables.value.Value;
import org.immutables.value.Value.Check;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;

@JsonDeserialize(as = ImmutableLeaderConfig.class)
@JsonSerialize(as = ImmutableLeaderConfig.class)
@Value.Immutable
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

    @Size(min=1)
    public abstract Set<String> leaders();

    @Value.Default
    public long pingRateMs() {
        return 5000l;
    }

    @Value.Default
    public long randomWaitBeforeProposingLeadershipMs() {
        return 1000l;
    }

    @Value.Default
    public long leaderPingResponseWaitMs() {
        return 5000l;
    }

    public abstract Map<String, String> additionalPaxosEndpointsToLogDir();

    @Check
    protected final void check() {
        Preconditions.checkArgument(leaders().contains(localServer()),
                "The localServer '%s' must included in the leader entries %s.", localServer(), leaders());
        Preconditions.checkArgument(learnerLogDir().exists() || learnerLogDir().mkdirs(),
                "Learner log directory '%s' does not exist and cannot be created.", learnerLogDir());
        Preconditions.checkArgument(acceptorLogDir().exists() || acceptorLogDir().mkdirs(),
                "Acceptor log directory '%s' does not exist and cannot be created.", acceptorLogDir());
        Preconditions.checkArgument(quorumSize() > leaders().size() / 2,
                "Quorum must be a majority.");
    }

}
