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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.performance.backend.DockerizedDatabaseUri;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.immutables.value.Value;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.util.Multiset;
import org.openjdk.jmh.util.Statistics;
import org.openjdk.jmh.util.TreeMultiset;

public class PerformanceResults {
    @VisibleForTesting
    static final String KVS_AGNOSTIC_SUFFIX = "N/A";

    private final Collection<RunResult> results;
    public static final int DOWNSAMPLE_MAXIMUM_SIZE = 500;

    public PerformanceResults(Collection<RunResult> results) {
        this.results = results;
    }

    public void writeToFile(File file) throws IOException {
        try (BufferedWriter fout = openFileWriter(file)) {
            List<ImmutablePerformanceResult> newResults = getPerformanceResults(results);
            new ObjectMapper().writeValue(fout, newResults);
        }
    }

    private static List<ImmutablePerformanceResult> getPerformanceResults(Collection<RunResult> results) {
        long date = System.currentTimeMillis();
        return results.stream()
                .map(rs -> ImmutablePerformanceResult.builder()
                        .date(date)
                        .benchmark(getBenchmarkName(rs.getParams()))
                        .samples(rs.getPrimaryResult().getStatistics().getN())
                        .std(rs.getPrimaryResult().getStatistics().getStandardDeviation())
                        .mean(rs.getPrimaryResult().getStatistics().getMean())
                        .data(getData(rs))
                        .units(rs.getParams().getTimeUnit())
                        .p50(rs.getPrimaryResult().getStatistics().getPercentile(50.0))
                        .p90(rs.getPrimaryResult().getStatistics().getPercentile(90.0))
                        .p99(rs.getPrimaryResult().getStatistics().getPercentile(99.0))
                        .build())
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    static String getBenchmarkName(BenchmarkParams params) {
        Optional<String> benchmarkUriSuffix = Optional.ofNullable(params.getParam(BenchmarkParam.URI.getKey()))
                .map(DockerizedDatabaseUri::fromUriString)
                .map(uri -> uri.getKeyValueServiceInstrumentation().toString());
        return formatBenchmarkString(params.getBenchmark(), benchmarkUriSuffix);
    }

    private static String formatBenchmarkString(String benchmark, Optional<String> uriSuffix) {
        List<String> benchmarkParts = Splitter.on('.').splitToList(benchmark);
        String benchmarkSuite = benchmarkParts.get(benchmarkParts.size() - 2);
        String benchmarkName = benchmarkParts.get(benchmarkParts.size() - 1);

        return String.format("%s#%s-%s", benchmarkSuite, benchmarkName, uriSuffix.orElse(KVS_AGNOSTIC_SUFFIX));
    }

    private static BufferedWriter openFileWriter(File file) throws FileNotFoundException {
        return new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8));
    }

    @VisibleForTesting
    static List<Double> getData(RunResult result) {
        return getRawResults(result.getPrimaryResult().getStatistics());
    }

    private static List<Double> getRawResults(Statistics statistics) {

        try {
            Field field = statistics.getClass().getDeclaredField("values");
            field.setAccessible(true);
            Multiset<Double> rawResults = (Multiset<Double>) field.get(statistics);
            return downSample(rawResults);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            String msg = "Could not get values from statistics !\n\t statistics.class = "
                    + statistics.getClass().getName() + "\n\t statistics.size = " + statistics.getN();
            throw new RuntimeException(msg, e);
        }
    }

    private static List<Double> downSample(Multiset<Double> multisetParam) {

        final TreeMultiset<Double> values = asTreeMultiset(multisetParam);
        final long totalCount = values.size();

        if (totalCount <= DOWNSAMPLE_MAXIMUM_SIZE) {
            return convertToList(values);
        }

        final List<Double> list = new ArrayList<>();
        int current = 0;

        for (double d : values.keys()) {
            current += Ints.saturatedCast(values.count(d));
            while (1.0 * DOWNSAMPLE_MAXIMUM_SIZE * current / totalCount >= (list.size() + 1)) {
                list.add(d);
            }
        }
        return list;
    }

    private static List<Double> convertToList(TreeMultiset<Double> values) {
        final List<Double> list = new ArrayList<>();
        for (double d : values.keys()) {
            for (int i = 0; i < values.count(d); ++i) {
                list.add(d);
            }
        }
        return list;
    }

    private static TreeMultiset<Double> asTreeMultiset(Multiset<Double> multisetParam) {
        if (multisetParam instanceof TreeMultiset) {
            return (TreeMultiset<Double>) multisetParam;
        } else {
            TreeMultiset<Double> values = new TreeMultiset<>();
            multisetParam.keys().forEach(key -> values.add(key, multisetParam.count(key)));
            return values;
        }
    }

    @JsonDeserialize(as = ImmutablePerformanceResult.class)
    @JsonSerialize(as = ImmutablePerformanceResult.class)
    @Value.Immutable
    abstract static class PerformanceResult {
        public abstract long date();

        public abstract String benchmark();

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
