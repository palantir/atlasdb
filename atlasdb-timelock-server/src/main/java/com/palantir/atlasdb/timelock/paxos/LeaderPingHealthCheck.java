/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.timelock.paxos;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.codahale.metrics.health.HealthCheck;
import com.palantir.leader.PingableLeader;

public class LeaderPingHealthCheck extends HealthCheck {
    private enum PingResult {
        SUCCESS,
        FAILURE,
        EXCEPTION
    }

    private Set<PingableLeader> leaders;

    public LeaderPingHealthCheck(Set<PingableLeader> leaders) {
        this.leaders = leaders;
    }

    @Override
    protected Result check() throws Exception {
        Map<PingResult, Long> pingResults = leaders.stream()
                .map(this::pingRecordingException)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        int nonExceptionResponses = leaders.size() - getCountPingResults(PingResult.EXCEPTION, pingResults);
        if (nonExceptionResponses < getQuorumSize()) {
            return Result.unhealthy("Less than a quorum of nodes responded to a ping request. "
                    + "We received a response from %s nodes after pinging %s nodes",
                    nonExceptionResponses, leaders.size());
        }

        int numLeaders = getCountPingResults(PingResult.SUCCESS, pingResults);
        if (numLeaders == 1) {
            return Result.healthy("There is exactly one leader in the Paxos cluster.");
        } else if (numLeaders == 0) {
            return Result.unhealthy("There are no leaders in the Paxos cluster.");
        } else {
            return Result.unhealthy("There are multiple leaders in the Paxos cluster. Found %s leaders", numLeaders);
        }
    }

    private int getQuorumSize() {
        return (leaders.size() + 1) / 2;
    }

    private int getCountPingResults(PingResult value, Map<PingResult, Long> pingResults) {
        if (pingResults.containsKey(value)) {
            return Math.toIntExact(pingResults.get(value));
        }
        return 0;
    }

    private PingResult pingRecordingException(PingableLeader leader) {
        try {
            return leader.ping() ? PingResult.SUCCESS : PingResult.FAILURE;
        } catch (Exception ex) {
            return PingResult.EXCEPTION;
        }
    }
}
