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
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

public final class LeaderConfig {

    private final int quorumSize;
    private final File learnerLogDir;
    private final File acceptorLogDir;
    private final String localServer;
    private final Set<String> leaders;
    private final long pingRateMs;
    private final long randomWaitBeforeProposingLeadershipMs;
    private final long leaderPingResponseWaitMs;

    public LeaderConfig(
            @JsonProperty("quorumSize") int quorumSize,
            @JsonProperty("learnerLogDir") Optional<File> learnerLogDir,
            @JsonProperty("acceptorLogDir") Optional<File> acceptorLogDir,
            @JsonProperty("localServer") String localServer,
            @JsonProperty("leaders") Set<String> leaders,
            @JsonProperty("pingRateMs") Optional<Long> pingRateMs,
            @JsonProperty("randomWaitBeforeProposingLeadershipMs") Optional<Long> randomWaitBeforeProposingLeadershipMs,
            @JsonProperty("leaderPingResponseWaitMs") Optional<Long> leaderPingResponseWaitMs) {
        Preconditions.checkArgument(leaders.contains(localServer),
                "The localServer must included in the leader entries.");

        this.quorumSize = quorumSize;
        this.learnerLogDir = learnerLogDir.or(new File("var/data/paxos/learner"));
        this.acceptorLogDir = acceptorLogDir.or(new File("var/data/paxos/acceptor"));
        this.localServer = localServer;
        this.leaders = leaders;
        this.pingRateMs = pingRateMs.or(5000l);
        this.randomWaitBeforeProposingLeadershipMs = randomWaitBeforeProposingLeadershipMs.or(1000l);
        this.leaderPingResponseWaitMs = leaderPingResponseWaitMs.or(this.pingRateMs);

        Preconditions.checkArgument(this.learnerLogDir.exists() || this.learnerLogDir.mkdirs(),
                "Learner log directory does not exist and cannot be created.");
        Preconditions.checkArgument(this.acceptorLogDir.exists() || this.acceptorLogDir.mkdirs(),
                "Acceptor log directory does not exist and cannot be created.");
    }

    public int getQuorumSize() {
        return quorumSize;
    }

    public File getLearnerLogDir() {
        return learnerLogDir;
    }

    public File getAcceptorLogDir() {
        return acceptorLogDir;
    }

    public String getLocalServer() {
        return localServer;
    }

    public Set<String> getLeaders() {
        return leaders;
    }
    
    public long getPingRateMs() {
        return pingRateMs;
    }
    
    public long getRandomWaitBeforeProposingLeadershipMs() {
        return randomWaitBeforeProposingLeadershipMs;
    }
    
    public long getLeaderPingResponseWaitMs() {
        return leaderPingResponseWaitMs;
    }
    
}
