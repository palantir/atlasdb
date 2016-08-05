package com.palantir.atlasdb.performance;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.immutables.value.Value;
import org.openjdk.jmh.results.RunResult;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

public class PerformanceResults {

    private final Collection<RunResult> results;

    public PerformanceResults(Collection<RunResult> results) {
        this.results = results;
    }

    public void writeToFile(File file) throws IOException {
        try (BufferedWriter fout = new BufferedWriter(new FileWriter(file))) {
            List<ImmutablePerformanceResult> newResults = results.stream().map(r -> {
                String[] benchmarkParts = r.getParams().getBenchmark().split("\\.");
                String benchmarkSuite = benchmarkParts[benchmarkParts.length - 2];
                String benchmarkName = benchmarkParts[benchmarkParts.length - 1];
                return ImmutablePerformanceResult.builder()
                        .date(System.currentTimeMillis())
                        .suite(benchmarkSuite)
                        .benchmark(benchmarkName)
                        .backend(r.getParams().getParam((BenchmarkParam.BACKEND.getKey())))
                        .samples(r.getPrimaryResult().getStatistics().getN())
                        .std(r.getPrimaryResult().getStatistics().getStandardDeviation())
                        .mean(r.getPrimaryResult().getStatistics().getMean())
                        .units(r.getParams().getTimeUnit())
                        .build();
            }).collect(Collectors.toList());
            new ObjectMapper().writeValue(fout, newResults);
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
        public abstract TimeUnit units();
    }

}
