package com.palantir.atlasdb.performance;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
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

public class PerformanceResults {

    private final Collection<RunResult> results;

    public PerformanceResults(Collection<RunResult> results) {
        this.results = results;
    }

    public void writeToFile(File file) throws IOException {
        try (BufferedWriter fout = new BufferedWriter(new FileWriter(file))) {
            long date = System.currentTimeMillis();
            List<ImmutablePerformanceResult> newResults = results.stream().map(r -> {
                String[] benchmarkParts = r.getParams().getBenchmark().split("\\.");
                String benchmarkSuite = benchmarkParts[benchmarkParts.length - 2];
                String benchmarkName = benchmarkParts[benchmarkParts.length - 1];
                return ImmutablePerformanceResult.builder()
                        .date(date)
                        .suite(benchmarkSuite)
                        .benchmark(benchmarkName)
                        .backend(r.getParams().getParam((BenchmarkParam.BACKEND.getKey())))
                        .samples(r.getPrimaryResult().getStatistics().getN())
                        .std(r.getPrimaryResult().getStatistics().getStandardDeviation())
                        .mean(r.getPrimaryResult().getStatistics().getMean())
                        .data(getData(r))
                        .units(r.getParams().getTimeUnit())
                        .p50(r.getPrimaryResult().getStatistics().getPercentile(50.0))
                        .p90(r.getPrimaryResult().getStatistics().getPercentile(90.0))
                        .p99(r.getPrimaryResult().getStatistics().getPercentile(99.0))
                        .build();
            }).collect(Collectors.toList());
            new ObjectMapper().writeValue(fout, newResults);
        }
    }

    private List<Double> getData(RunResult result) {
        return result.getBenchmarkResults().stream()
                .flatMap(b -> getRawResults(b.getPrimaryResult().getStatistics()).stream())
                .collect(Collectors.toList());
    }

    private List<Double> getRawResults(Statistics statistics) {
        try {
            Field f = statistics.getClass().getDeclaredField("values");
            f.setAccessible(true);
            Multiset<Double> rawResults = (Multiset<Double>) f.get(statistics);
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
    static abstract class PerformanceResult {
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
