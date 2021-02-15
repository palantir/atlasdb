/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.health;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.transaction.api.TimelockServiceStatus;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.tritium.metrics.registry.MetricName;
import java.util.Map;

public class MetricsBasedTimelockHealthCheck implements TimelockHealthCheck {
    private final MetricsManager metricsManager;

    public MetricsBasedTimelockHealthCheck(MetricsManager metricsManager) {
        this.metricsManager = metricsManager;
    }

    @Override
    public TimelockServiceStatus getStatus() {
        Map<MetricName, Metric> metrics = metricsManager.getTaggedRegistry().getMetrics();
        Metric success = metrics.get(MetricName.builder()
                .safeName(AtlasDbMetricNames.TIMELOCK_SUCCESSFUL_REQUEST)
                .build());
        Metric failure = metrics.get(MetricName.builder()
                .safeName(AtlasDbMetricNames.TIMELOCK_FAILED_REQUEST)
                .build());

        if (!(success instanceof Meter) || !(failure instanceof Meter)) {
            throw new SafeIllegalStateException("Timelock client metrics is not properly set");
        }

        double successfulRequestRate = ((Meter) success).getFiveMinuteRate();
        double failedRequestRate = ((Meter) failure).getFiveMinuteRate();
        if (successfulRequestRate >= failedRequestRate) {
            return TimelockServiceStatus.HEALTHY;
        } else {
            return TimelockServiceStatus.UNHEALTHY;
        }
    }
}
