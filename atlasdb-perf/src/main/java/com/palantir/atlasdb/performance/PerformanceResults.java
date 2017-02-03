/**
 * Copyright 2016 Palantir Technologies
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
 *
 */

package com.palantir.atlasdb.performance;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import org.immutables.value.Value;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.util.Multiset;
import org.openjdk.jmh.util.Statistics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.performance.backend.DockerizedDatabaseUri;

public class PerformanceResults {

    private final Collection<RunResult> results;

    public PerformanceResults(Collection<RunResult> results) {
        this.results = results;
    }

    public void writeToFile(File file) throws IOException {
        try (BufferedWriter fout = openFileWriter(file)) {
            long date = System.currentTimeMillis();
            List<ImmutablePerformanceResult> newResults = results.stream().map(rs -> {
                String[] benchmarkParts = rs.getParams().getBenchmark().split("\\.");
                String benchmarkSuite = benchmarkParts[benchmarkParts.length - 2];
                String benchmarkName = benchmarkParts[benchmarkParts.length - 1];
                DockerizedDatabaseUri uri =
                        DockerizedDatabaseUri.fromUriString(rs.getParams().getParam(BenchmarkParam.URI.getKey()));
                return ImmutablePerformanceResult.builder()
                        .date(date)
                        .suite(benchmarkSuite)
                        .benchmark(benchmarkName)
                        .backend(uri.getKeyValueServiceInstrumentation().toString())
                        .samples(rs.getPrimaryResult().getStatistics().getN())
                        .std(rs.getPrimaryResult().getStatistics().getStandardDeviation())
                        .mean(rs.getPrimaryResult().getStatistics().getMean())
                        .data(getData(rs))
                        .units(rs.getParams().getTimeUnit())
                        .p50(rs.getPrimaryResult().getStatistics().getPercentile(50.0))
                        .p90(rs.getPrimaryResult().getStatistics().getPercentile(90.0))
                        .p99(rs.getPrimaryResult().getStatistics().getPercentile(99.0))
                        .build();
            }).collect(Collectors.toList());
            new ObjectMapper().writeValue(fout, newResults);
        }
    }

    private String getBenchmarkStr(String benchmark, DockerizedDatabaseUri uri) {
        String[] benchmarkParts = benchmark.split("\\.");
        String benchmarkSuite = benchmarkParts[benchmarkParts.length - 2];
        String benchmarkName = benchmarkParts[benchmarkParts.length - 1];
        return benchmarkSuite + "#" + benchmarkName + "-" + uri.getKeyValueServiceInstrumentation().toString();
    }

    private BufferedWriter openFileWriter(File file) throws FileNotFoundException {
        return new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8));
    }

    private List<Double> getData(RunResult result) {
        return result.getBenchmarkResults().stream()
                .flatMap(b -> getRawResults(b.getPrimaryResult().getStatistics()).stream())
                .collect(Collectors.toList());
    }

    private List<Double> getRawResults(Statistics statistics) {
        try {
            Field field = statistics.getClass().getDeclaredField("values");
            field.setAccessible(true);
            Multiset<Double> rawResults = (Multiset<Double>) field.get(statistics);
            return rawResults.entrySet().stream()
                    .flatMap(e -> DoubleStream.iterate(e.getKey(), d -> d).limit(e.getValue()).boxed())
                    .collect(Collectors.toList());
        } catch (NoSuchFieldException | IllegalAccessException e) {
            return Lists.newArrayList();
        }
    }

    @JsonDeserialize(as = ImmutablePerformanceResult.class)
    @JsonSerialize(as = ImmutablePerformanceResult.class)
    @Value.Immutable
    abstract static class PerformanceResult {
        public abstract long date();
        public abstract String suite();
        public abstract String benchmark();
        public abstract String backend();
        public abstract long samples();
        public abstract double std();
        public abstract double mean();
        public abstract List<Double> data();
        public abstract TimeUnit units();
        public abstract double p50();
        public abstract double p90();
        public abstract double p99();
    }

}
