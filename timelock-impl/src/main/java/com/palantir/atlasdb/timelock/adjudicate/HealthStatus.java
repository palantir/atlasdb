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

import java.util.HashSet;

import com.google.common.collect.Sets;

public enum HealthStatus {
    HEALTHY(true),
    UNKNOWN(true),
    UNHEALTHY(false);

    private final boolean isHealthy;

    HealthStatus(boolean isHealthy) {
        this.isHealthy = isHealthy;
    }

    public boolean isHealthy() {
        return isHealthy;
    }

    public static HealthStatus getWorseState(HealthStatus... statuses) {
        HashSet<HealthStatus> healthStatusSet = Sets.newHashSet(statuses);
        return healthStatusSet.contains(HealthStatus.UNHEALTHY) ? HealthStatus.UNHEALTHY
                : (healthStatusSet.contains(HealthStatus.UNKNOWN) ? HealthStatus.UNKNOWN : HealthStatus.HEALTHY);
    }
}
