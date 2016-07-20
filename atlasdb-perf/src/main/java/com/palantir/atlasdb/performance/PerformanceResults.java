package com.palantir.atlasdb.performance;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;

import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.util.Statistics;

public class PerformanceResults {

    private static final double CONFIDENCE_INTERVAL = 0.975;

    private final Collection<RunResult> results;

    public PerformanceResults(Collection<RunResult> results) {
        this.results = results;
    }

    public void writeToFile(File file) throws IOException {
        try (BufferedWriter fout = new BufferedWriter(new FileWriter(file))) {
            fout.write("suite, benchmark, backend, trials, mean, error, units\n");
            for (RunResult r : results) {
                Statistics stats = r.getPrimaryResult().getStatistics();
                String[] benchmarkParts = r.getParams().getBenchmark().split("\\.");
                String benchmarkSuite = benchmarkParts[benchmarkParts.length - 2];
                String benchmarkName = benchmarkParts[benchmarkParts.length - 1];
                fout.write(
                        String.format("%s, %s, %s, %s, %s, %s, %s\n",
                                benchmarkSuite,
                                benchmarkName,
                                r.getParams().getParam(BenchmarkParam.BACKEND.getKey()),
                                stats.getN(),
                                stats.getMean(),
                                stats.getMeanErrorAt(CONFIDENCE_INTERVAL),
                                r.getParams().getTimeUnit())
                );
            }
        }
    }
}
