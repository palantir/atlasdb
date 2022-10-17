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

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.util.MetricsManagers;
import java.time.Duration;
import org.junit.Before;
import org.junit.Test;

public class CassandraTopologyValidationMetricsTest {

    private CassandraTopologyValidationMetrics metrics;

    @Before
    public void before() {
        metrics = new CassandraTopologyValidationMetrics(MetricsManagers.createForTests());
    }

    @Test
    public void markIncrementsByOne() {
        metrics.markTopologyValidationFailure();
        metrics.markTopologyValidationFailure();
        assertThat(metrics.getTopologyValidationFailures().getCount()).isEqualTo(2);
    }

    @Test
    public void recordLatency() {
        long latencyInMillis = 1000L;
        metrics.recordTopologyValidationLatency(Duration.ofMillis(latencyInMillis));
        assertThat(metrics.getTopologyValidationLatency().getCount()).isEqualTo(1);
        assertThat(metrics.getTopologyValidationLatency().getSnapshot().getValues())
                .containsExactly(latencyInMillis);
    }
}
