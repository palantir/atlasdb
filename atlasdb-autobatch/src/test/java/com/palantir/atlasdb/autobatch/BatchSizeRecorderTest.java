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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.codahale.metrics.Histogram;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;

public class BatchSizeRecorderTest {
    private static final String SAFE_IDENTIFIER = "identifier";

    @Test
    public void metersResults() {
        BatchSizeRecorder batchSizeRecorder = BatchSizeRecorder.create(SAFE_IDENTIFIER);
        batchSizeRecorder.markBatchProcessed(5);
        batchSizeRecorder.markBatchProcessed(10);

        Histogram histogram = (Histogram) SharedTaggedMetricRegistries.getSingleton().getMetrics()
                .get(MetricName.builder().safeName(BatchSizeRecorder.AUTOBATCHER_METER)
                        .putSafeTags("identifier", SAFE_IDENTIFIER)
                        .build());

        assertThat(histogram).isNotNull();
        assertThat(histogram.getCount()).isEqualTo(2);
        assertThat(histogram.getSnapshot().getMean()).isEqualTo(7.5);
    }

}
