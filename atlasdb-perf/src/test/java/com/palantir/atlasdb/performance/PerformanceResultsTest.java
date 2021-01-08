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
package com.palantir.atlasdb.performance;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.performance.backend.CassandraKeyValueServiceInstrumentation;
import java.util.List;
import org.apache.commons.math3.stat.inference.TestUtils;
import org.assertj.core.util.Lists;
import org.junit.Test;
import org.mockito.Mockito;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.WorkloadParams;
import org.openjdk.jmh.util.MultisetStatistics;

public class PerformanceResultsTest {
    private static final String SUITE_NAME = "PerformanceResults";
    private static final String BENCHMARK_NAME = "doStuff";
    private static final String FULL_BENCHMARK_NAME =
            "com.palantir.atlasdb.performance." + SUITE_NAME + "." + BENCHMARK_NAME;
    private static final String FORMATTED_BENCHMARK_NAME = SUITE_NAME + "#" + BENCHMARK_NAME;

    private static final String FORMATTED_BENCHMARK_NAME_AGNOSTIC =
            FORMATTED_BENCHMARK_NAME + "-" + PerformanceResults.KVS_AGNOSTIC_SUFFIX;

    private static final String DOCKERIZED_CASSANDRA_URI =
            CassandraKeyValueServiceInstrumentation.class.getCanonicalName() + "@192.168.99.100:9160";
    private static final String FORMATTED_BENCHMARK_NAME_CASSANDRA =
            FORMATTED_BENCHMARK_NAME + "-" + new CassandraKeyValueServiceInstrumentation().toString();

    private static final RunResult mockRunResult = Mockito.mock(RunResult.class);
    private static final Result mockResult = Mockito.mock(Result.class);
    private static final List<Double> SMALL_SAMPLE = Lists.newArrayList();
    private static final List<Double> LARGE_SAMPLE = Lists.newArrayList();

    static {
        Mockito.when(mockRunResult.getPrimaryResult()).thenReturn(mockResult);
        for (int i = 0; i < PerformanceResults.DOWNSAMPLE_MAXIMUM_SIZE; ++i) {
            SMALL_SAMPLE.add(Double.valueOf(i));
            LARGE_SAMPLE.add(Double.valueOf(3 * i));
            LARGE_SAMPLE.add(Double.valueOf(3 * i + 1));
            LARGE_SAMPLE.add(Double.valueOf(3 * i + 2));
        }
    }

    @Test
    public void canGenerateBenchmarkNameForTestWithoutKeyValueService() {
        BenchmarkParams params = createBenchmarkParams(FULL_BENCHMARK_NAME, "foo", "bar");

        assertThat(PerformanceResults.getBenchmarkName(params)).isEqualTo(FORMATTED_BENCHMARK_NAME_AGNOSTIC);
    }

    @Test
    public void canGenerateBenchmarkNameForTestWithKeyValueService() {
        BenchmarkParams params = createBenchmarkParams(
                "PerformanceResults.doStuff", BenchmarkParam.URI.getKey(), DOCKERIZED_CASSANDRA_URI);

        assertThat(PerformanceResults.getBenchmarkName(params)).isEqualTo(FORMATTED_BENCHMARK_NAME_CASSANDRA);
    }

    @Test
    public void doesNotDownsampleSmallSample() {
        MultisetStatistics stats = new MultisetStatistics();
        stats.addValue(3.14, 1);
        Mockito.when(mockResult.getStatistics()).thenReturn(stats);
        assertThat(PerformanceResults.getData(mockRunResult)).containsExactlyElementsOf(ImmutableList.of(3.14));
    }

    @Test
    public void doesNotDownsampleSampleMaxsizeSample() {
        MultisetStatistics stats = new MultisetStatistics();
        for (double number : SMALL_SAMPLE) {
            stats.addValue(number, 1);
        }
        Mockito.when(mockResult.getStatistics()).thenReturn(stats);
        assertThat(PerformanceResults.getData(mockRunResult)).containsExactlyElementsOf(SMALL_SAMPLE);
    }

    @Test
    public void downSamplesCorrectlyWhenUniform() {
        MultisetStatistics stats = new MultisetStatistics();
        for (int i = 0; i < 10; ++i) {
            for (double number : SMALL_SAMPLE) {
                stats.addValue(number, 2);
            }
        }
        Mockito.when(mockResult.getStatistics()).thenReturn(stats);
        assertThat(PerformanceResults.getData(mockRunResult)).containsExactlyElementsOf(SMALL_SAMPLE);
    }

    @Test
    public void downSampledDistributionIsRepresentativeForReasonableData() {
        MultisetStatistics stats = new MultisetStatistics();
        for (double number : LARGE_SAMPLE) {
            int elements = (int) (10
                    * PerformanceResults.DOWNSAMPLE_MAXIMUM_SIZE
                    / (Math.abs(PerformanceResults.DOWNSAMPLE_MAXIMUM_SIZE - number) + 1));
            stats.addValue(number, elements);
        }
        Mockito.when(mockResult.getStatistics()).thenReturn(stats);
        List<Double> downSampledData = PerformanceResults.getData(mockRunResult);

        MultisetStatistics downSampledStats = new MultisetStatistics();
        for (double number : downSampledData) {
            downSampledStats.addValue(number, 1);
        }

        // Hypothesis that means are the same cannot be rejected with confidence more than 0.5
        assertThat(TestUtils.tTest(stats, downSampledStats, 1 - 0.5)).isFalse();
        // The typical p value is 0.05, but I went with 0.5 because I can
        assertThat(TestUtils.homoscedasticTTest(stats, downSampledStats)).isGreaterThan(0.5d);
    }

    private static BenchmarkParams createBenchmarkParams(String benchmarkName, String paramKey, String paramValue) {
        WorkloadParams workloadParams = new WorkloadParams();
        workloadParams.put(paramKey, paramValue, 0);

        // Sorry, JMH API doesn't have a builder. Isolating the badness to just here.
        return new BenchmarkParams(
                benchmarkName,
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
