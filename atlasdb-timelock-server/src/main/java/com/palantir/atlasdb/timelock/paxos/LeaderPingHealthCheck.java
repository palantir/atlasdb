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

import java.util.Set;

import com.codahale.metrics.health.HealthCheck;
import com.palantir.leader.PingableLeader;

public class LeaderPingHealthCheck extends HealthCheck {
    private Set<PingableLeader> leaders;

    public LeaderPingHealthCheck(Set<PingableLeader> leaders) {
        this.leaders = leaders;
    }

    @Override
    protected Result check() throws Exception {
        long numLeaders = leaders.stream()
                .map(PingableLeader::ping)
                .filter(pingResult -> pingResult == Boolean.TRUE)
                .count();

        if (numLeaders == 1) {
            return Result.healthy("There is exactly one leader in the Paxos cluster.");
        } else if (numLeaders == 0) {
            return Result.unhealthy("There are no leaders in the Paxos cluster.");
        } else {
            return Result.unhealthy("There are multiple leaders in the Paxos cluster. Found %s leaders", numLeaders);
        }
    }
}
