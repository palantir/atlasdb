/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.timelock.paxos;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.palantir.leader.PingableLeader;
import com.palantir.timelock.TimeLockStatus;

public class LeaderPingHealthCheck {
    private enum PingResult {
        SUCCESS,
        FAILURE,
        EXCEPTION
    }

    private Set<PingableLeader> leaders;

    public LeaderPingHealthCheck(Set<PingableLeader> leaders) {
        this.leaders = leaders;
    }

    public TimeLockStatus getStatus() {
        Map<PingResult, Long> pingResults = leaders.stream()
                .map(this::pingRecordingException)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        int nonExceptionResponses = leaders.size() - getCountPingResults(PingResult.EXCEPTION, pingResults);
        if (nonExceptionResponses < getQuorumSize()) {
            return TimeLockStatus.NO_QUORUM;
        }

        int numLeaders = getCountPingResults(PingResult.SUCCESS, pingResults);
        if (numLeaders == 1) {
            return TimeLockStatus.ONE_LEADER;
        } else if (numLeaders == 0) {
            return TimeLockStatus.NO_LEADER;
        } else {
            return TimeLockStatus.MULTIPLE_LEADERS;
        }
    }

    private int getQuorumSize() {
        return (leaders.size() + 1) / 2;
    }

    private int getCountPingResults(PingResult value, Map<PingResult, Long> pingResults) {
        return Math.toIntExact(pingResults.getOrDefault(value, 0L));
    }

    private PingResult pingRecordingException(PingableLeader leader) {
        try {
            return leader.ping() ? PingResult.SUCCESS : PingResult.FAILURE;
        } catch (Exception ex) {
            return PingResult.EXCEPTION;
        }
    }
}
