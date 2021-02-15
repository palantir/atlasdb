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

package com.palantir.atlasdb.keyvalue.cassandra.pool;

public enum CassandraClientPoolHostLevelMetric {
    MEAN_ACTIVE_TIME_MILLIS("meanActiveTimeMillis", 0.0, 2.0),
    NUM_IDLE("numIdle", 0.1, 2.0),
    NUM_ACTIVE("numActive", 0.1, 2.0),
    CREATED("created", 0.01, 2.0),
    DESTROYED_BY_EVICTOR("destroyedByEvictor", 0.01, 2.0),
    EVICTOR_TASK_SIZE("evictorTaskSize", 0.0, 100.0);

    public final String metricName;
    public final double minimumMeanThreshold;
    public final double maximumMeanThreshold;

    CassandraClientPoolHostLevelMetric(String metricName, double minimumMeanThreshold, double maximumMeanThreshold) {
        this.metricName = metricName;
        this.minimumMeanThreshold = minimumMeanThreshold;
        this.maximumMeanThreshold = maximumMeanThreshold;
    }
}
