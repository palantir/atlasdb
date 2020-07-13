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

import java.util.Comparator;

public enum HealthStatus {
    HEALTHY(true, 1),
    UNKNOWN(true, 2),
    UNHEALTHY(false, 3);

    private final boolean isHealthy;
    private final int severity;

    HealthStatus(boolean isHealthy, int severity) {
        this.isHealthy = isHealthy;
        this.severity = severity;
    }

    public boolean isHealthy() {
        return isHealthy;
    }

    public static Comparator<HealthStatus> getHealthStatusComparator() {
        return Comparator.comparingLong(HealthStatus::severity);
    }

    private static long severity(HealthStatus status) {
        return status.severity;
    }
}
