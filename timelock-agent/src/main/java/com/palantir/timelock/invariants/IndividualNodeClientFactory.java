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

package com.palantir.timelock.invariants;

import java.util.List;

import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timelock.config.ClusterConfiguration;

public class IndividualNodeClientFactory {
    private final ClusterConfiguration timelockClusterConfiguration;

    public IndividualNodeClientFactory(
            ClusterConfiguration timelockClusterConfiguration) {
        this.timelockClusterConfiguration = timelockClusterConfiguration;
    }

    private List<TimelockService> getTimelockServicesForClient(String client) {
        timelockClusterConfiguration.clusterMembers()
                .stream()
                .map(t -> t + "/" + client)
                .map(t -> AtlasDbHttpClients.createProxy(
                        null,

                ))
    }
}
