/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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
import com.palantir.atlasdb.autobatch.CoalescingRequestFunction;
import com.palantir.atlasdb.timelock.paxos.AutobatchingPingableLeaderFactory.PingRequest;
import com.palantir.paxos.Client;
import com.palantir.paxos.LeaderPingResult;
import com.palantir.paxos.LeaderPingResults;
import com.palantir.paxos.LeaderPingerContext;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

final class PingCoalescingFunction implements CoalescingRequestFunction<PingRequest, LeaderPingResult> {

    private final LeaderPingerContext<BatchPingableLeader> batchPingableLeader;

    PingCoalescingFunction(LeaderPingerContext<BatchPingableLeader> batchPingableLeader) {
        this.batchPingableLeader = batchPingableLeader;
    }

    @Override
    public Map<PingRequest, LeaderPingResult> apply(Set<PingRequest> requests) {
        Set<Client> clientRequests = requests.stream().map(PingRequest::client).collect(Collectors.toSet());
        Set<Client> pingResults = batchPingableLeader.pinger().ping(clientRequests);
        return Maps.toMap(requests, client -> getLeaderPingResult(pingResults, client));
    }

    private LeaderPingResult getLeaderPingResult(Set<Client> pingResults, PingRequest currentRequest) {
        if (pingResults.contains(currentRequest.client())) {
            return LeaderPingResults.pingReturnedTrue(
                    currentRequest.requestedLeaderId(),
                    batchPingableLeader.hostAndPort());
        } else {
            return LeaderPingResults.pingReturnedFalse();
        }
    }
}
