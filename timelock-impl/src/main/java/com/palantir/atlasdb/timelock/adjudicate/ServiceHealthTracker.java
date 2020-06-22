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

package com.palantir.atlasdb.timelock.adjudicate;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.immutables.value.Value;


public class ServiceHealthTracker {

    static HealthStatus getHealthStatus(Service service) {
        List<HealthStatus> majorityHealthStatusList =
                Utils.getMajority(service.nodes().values().stream().map(NodeHealthTracker::getHealthStatus));
        if (majorityHealthStatusList.size() == 1) {
            return majorityHealthStatusList.get(0);
        }
        return HealthStatus.IGNORED;
    }

    @Value.Immutable
    interface Service {
        String serviceName();

        Map<UUID, NodeHealthTracker.Node> nodes();
    }
}
