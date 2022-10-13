/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.util.MetricsManager;
import java.time.Duration;

public class CassandraTopologyValidationMetrics {
    private final Counter topologyValidationFailures;
    private final Histogram topologyValidationLatency;

    public CassandraTopologyValidationMetrics(MetricsManager metricsManager) {
        this.topologyValidationFailures = metricsManager.registerOrGetCounter(
                CassandraTopologyValidationMetrics.class, "topologyValidationFailures");
        this.topologyValidationLatency = metricsManager.registerOrGetHistogram(
                CassandraTopologyValidationMetrics.class, "topologyValidationLatency");
    }

    public void markTopologyValidationFailure() {
        topologyValidationFailures.inc();
    }

    public void recordTopologyValidationLatency(Duration duration) {
        topologyValidationLatency.update(duration.toMillis());
    }

    @VisibleForTesting
    public Counter getTopologyValidationFailures() {
        return topologyValidationFailures;
    }

    @VisibleForTesting
    public Histogram getTopologyValidationLatency() {
        return topologyValidationLatency;
    }
}
