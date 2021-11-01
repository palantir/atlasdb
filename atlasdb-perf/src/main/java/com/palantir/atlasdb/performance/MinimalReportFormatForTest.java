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

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.results.BenchmarkResult;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.format.OutputFormat;

public final class MinimalReportFormatForTest implements OutputFormat {
    private static final PrintStream out = System.out;

    private static final MinimalReportFormatForTest INSTANCE = new MinimalReportFormatForTest();

    private MinimalReportFormatForTest() {}

    public static MinimalReportFormatForTest get() {
        return INSTANCE;
    }

    @Override
    public void iteration(BenchmarkParams _benchParams, IterationParams _params, int _iteration) {}

    @Override
    public void iterationResult(
            BenchmarkParams _benchParams, IterationParams _params, int _iteration, IterationResult _data) {}

    @Override
    public void startBenchmark(BenchmarkParams benchParams) {
        out.println();
        out.println("# Starting Benchmark: " + benchParams.getBenchmark());
    }

    @Override
    public void endBenchmark(BenchmarkResult result) {
        out.println();
        out.println("# Finished Benchmark: " + result.getParams().getBenchmark());
    }

    @Override
    public void startRun() {}

    @Override
    public void endRun(Collection<RunResult> _result) {}

    @Override
    public void print(String _str) {}

    @Override
    public void println(String _str) {}

    @Override
    public void flush() {}

    @Override
    public void close() {}

    @Override
    public void verbosePrintln(String _str) {}

    @Override
    public void write(int _num) {}

    @Override
    public void write(byte[] _num) throws IOException {}
}
