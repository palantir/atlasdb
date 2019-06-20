/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.autobatch;

import javax.annotation.concurrent.NotThreadSafe;

import com.codahale.metrics.Meter;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;

@NotThreadSafe // Disruptor runs the batching function on just one thread.
public final class BatchSizeRecorder {
    static final String AUTOBATCHER_METER = "autobatcherMeter";

    private final Meter meter;

    private BatchSizeRecorder(Meter meter) {
        this.meter = meter;
    }

    public static BatchSizeRecorder create(String safeLoggerIdentifier) {
        Meter meter = SharedTaggedMetricRegistries.getSingleton().meter(MetricName.builder()
                        .safeName(AUTOBATCHER_METER)
                        .putSafeTags("safeIdentifier", safeLoggerIdentifier)
                        .build());
        return new BatchSizeRecorder(meter);
    }

    public void markBatchProcessed(long batchSize) {
        meter.mark(batchSize);
    }

}
