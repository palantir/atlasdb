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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe // Disruptor runs the batching function on just one thread.
public final class BatchSizeRecorder {
    static final String BATCH_SIZE_METER_NAME_PREFIX = BatchSizeRecorder.class.getName();

    private final Histogram histogram;

    private BatchSizeRecorder(Histogram histogram) {
        this.histogram = histogram;
    }

    public static BatchSizeRecorder create(String safeLoggerIdentifier, Map<String, String> tags) {
        Histogram histogram = SharedTaggedMetricRegistries.getSingleton()
                .histogram(MetricName.builder()
                        .safeName(BATCH_SIZE_METER_NAME_PREFIX + ".batchSize")
                        .putSafeTags("identifier", safeLoggerIdentifier)
                        .putAllSafeTags(tags)
                        .build());

        Gauge<Double> medianGauge = () -> histogram.getSnapshot().getMedian();
        SharedTaggedMetricRegistries.getSingleton()
                .registerWithReplacement(
                        MetricName.builder()
                                .safeName(BATCH_SIZE_METER_NAME_PREFIX + ".batchSizeMedian")
                                .putSafeTags("identifier", safeLoggerIdentifier)
                                .putAllSafeTags(tags)
                                .build(),
                        medianGauge);

        return new BatchSizeRecorder(histogram);
    }

    public void markBatchDispatched(long batchSize) {
        histogram.update(batchSize);
    }
}
