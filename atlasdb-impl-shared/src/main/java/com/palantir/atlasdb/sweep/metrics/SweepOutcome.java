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
package com.palantir.atlasdb.sweep.metrics;

public enum SweepOutcome {
    SUCCESS(0),
    NOTHING_TO_SWEEP(1),
    UNABLE_TO_ACQUIRE_LOCKS(2),
    NOT_ENOUGH_DB_NODES_ONLINE(3),
    TABLE_DROPPED_WHILE_SWEEPING(4),
    ERROR(5),
    FATAL(6);

    private final int metricsIntRepresentation;

    SweepOutcome(int metricsIntRepresentation) {
        this.metricsIntRepresentation = metricsIntRepresentation;
    }

    int getMetricsIntRepresentation() {
        return metricsIntRepresentation;
    }
}
