/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.paxos;

import com.google.common.collect.Maps;
import com.palantir.leader.PingableLeader;
import com.palantir.paxos.Client;
import com.palantir.timelock.paxos.HealthCheckPinger;
import com.palantir.timelock.paxos.HealthCheckResponse;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SingleLeaderHealthCheckPinger implements HealthCheckPinger {
    private static final Logger log = LoggerFactory.getLogger(SingleLeaderHealthCheckPinger.class);

    private final PingableLeader pingableLeader;

    public SingleLeaderHealthCheckPinger(PingableLeader pingableLeader) {
        this.pingableLeader = pingableLeader;
    }

    @Override
    public Map<Client, HealthCheckResponse> apply(Set<Client> request) {
        boolean isLeader = pingableLeader.ping();
        return Maps.toMap(request, _client -> new HealthCheckResponse(isLeader));
    }

    public boolean safePing() {
        try {
            return pingableLeader.ping();
        } catch (Exception e) {
            log.info(
                    "Caught an exception when attempting to do a ping. If this is bad we expect exceptions elsewhere",
                    e);
            return false;
        }
    }
}
