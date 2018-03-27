/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.health;

import java.util.Map;

import com.codahale.metrics.Meter;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.transaction.api.TimelockServiceStatus;
import com.palantir.atlasdb.util.AtlasDbMetrics;

public class MetricsBasedTimelockHealthCheck implements TimelockHealthCheck{
    public TimelockServiceStatus getStatus() {
        Map<String, Meter> meters = AtlasDbMetrics.getMetricRegistry().getMeters();
        if (!meters.containsKey(AtlasDbMetricNames.TIMELOCK_SUCCESSFUL_REQUEST)
                || !meters.containsKey(AtlasDbMetricNames.TIMELOCK_FAILED_REQUEST)) {
            throw new IllegalStateException("Timelock client metrics is not properly set");
        }

        double successfulRequestRate = meters.get(AtlasDbMetricNames.TIMELOCK_SUCCESSFUL_REQUEST).getFiveMinuteRate();
        double failedRequestRate = meters.get(AtlasDbMetricNames.TIMELOCK_FAILED_REQUEST).getFiveMinuteRate();
        if (successfulRequestRate >= failedRequestRate) {
            return TimelockServiceStatus.HEALTHY;
        } else {
            return TimelockServiceStatus.UNHEALTHY;
        }
    }
}
