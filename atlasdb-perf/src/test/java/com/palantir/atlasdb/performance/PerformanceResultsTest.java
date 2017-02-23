/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.performance;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.runner.WorkloadParams;

import com.palantir.atlasdb.performance.backend.CassandraKeyValueServiceInstrumentation;

public class PerformanceResultsTest {
    private static final String SUITE_NAME = "PerformanceResults";
    private static final String BENCHMARK_NAME = "doStuff";
    private static final String FULL_BENCHMARK_NAME
            = "com.palantir.atlasdb.performance." + SUITE_NAME + "." + BENCHMARK_NAME;
    private static final String FORMATTED_BENCHMARK_NAME = SUITE_NAME + "#" + BENCHMARK_NAME;

    private static final String DOCKERIZED_CASSANDRA_URI
            = CassandraKeyValueServiceInstrumentation.class.getCanonicalName() + "@192.168.99.100:9160";
    private static final String FORMATTED_BENCHMARK_NAME_CASSANDRA
            = FORMATTED_BENCHMARK_NAME + "-" + new CassandraKeyValueServiceInstrumentation().toString();

    @Test
    public void canGenerateBenchmarkNameForTestWithoutKeyValueService() {
        BenchmarkParams params = createBenchmarkParams(FULL_BENCHMARK_NAME, "foo", "bar");

        assertThat(PerformanceResults.getBenchmarkName(params)).isEqualTo(FORMATTED_BENCHMARK_NAME);
    }

    @Test
    public void canGenerateBenchmarkNameForTestWithKeyValueService() {
        BenchmarkParams params = createBenchmarkParams("PerformanceResults.doStuff",
                BenchmarkParam.URI.getKey(),
                DOCKERIZED_CASSANDRA_URI);

        assertThat(PerformanceResults.getBenchmarkName(params)).isEqualTo(FORMATTED_BENCHMARK_NAME_CASSANDRA);
    }

    private static BenchmarkParams createBenchmarkParams(String benchmarkName, String paramKey, String paramValue) {
        WorkloadParams workloadParams = new WorkloadParams();
        workloadParams.put(paramKey, paramValue, 0);

        // Sorry, JMH API doesn't have a builder. Isolating the badness to just here.
        return new BenchmarkParams(benchmarkName,
                null,
                false,
                0,
                null,
                null,
                1,
                1,
                null,
                null,
                null,
                workloadParams,
                null,
                1,
                null,
                null,
                null);
    }
}
