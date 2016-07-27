package com.palantir.atlasdb.performance;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.stream.Collectors;

import org.openjdk.jmh.results.RunResult;

public class PerformanceResults {

    private final Collection<RunResult> results;

    public PerformanceResults(Collection<RunResult> results) {
        this.results = results;
    }

    public void writeToFile(File file) throws IOException {
        try (BufferedWriter fout = new BufferedWriter(new FileWriter(file))) {
            fout.write("date, suite, benchmark, backend, runs, units\n");
            for (RunResult r : results) {
                String[] benchmarkParts = r.getParams().getBenchmark().split("\\.");
                String benchmarkSuite = benchmarkParts[benchmarkParts.length - 2];
                String benchmarkName = benchmarkParts[benchmarkParts.length - 1];
                fout.write(
                        String.format("%d, %s, %s, %s, [%s], %s\n",
                                System.currentTimeMillis(),
                                benchmarkSuite,
                                benchmarkName,
                                r.getParams().getParam(BenchmarkParam.BACKEND.getKey()),
                                getResultsFromRun(r)
                                        .stream()
                                        .map(d -> Double.toString(d))
                                        .collect(Collectors.joining(" ")),
                                r.getParams().getTimeUnit())
                );
            }
        }
    }

    private Collection<Double> getResultsFromRun(RunResult r) {
        return r.getBenchmarkResults()
                .stream()
                .flatMap(rb -> rb.getIterationResults().stream())
                .map(ir -> ir.getPrimaryResult().getScore())
                .collect(Collectors.toList());
    }

}
