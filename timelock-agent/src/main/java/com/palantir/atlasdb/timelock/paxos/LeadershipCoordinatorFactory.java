/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.proxy.LeadershipCoordinator;
import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LeadershipCoordinatorFactory implements Closeable {
    private final Map<LeaderElectionService, LeadershipCoordinator> leadershipCoordinators = new ConcurrentHashMap<>();

    public LeadershipCoordinator create(LeaderElectionService leaderElectionService) {
        return leadershipCoordinators.computeIfAbsent(leaderElectionService, LeadershipCoordinator::create);
    }

    @Override
    public void close() {
        leadershipCoordinators.keySet().forEach(leaderElectionService -> {
            LeadershipCoordinator leadershipCoordinator = leadershipCoordinators.remove(leaderElectionService);
            if (leadershipCoordinator != null) {
                leadershipCoordinator.close();
            }
        });
    }
}
