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

package com.palantir.atlasdb.performance.cli;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.reflections.Reflections;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.HelpOption;
import io.airlift.airline.Option;
import io.airlift.airline.SingleCommand;

/**
 * The Atlas Perf(ormance) CLI is a tool for making and running AtlasDB performance tests.
 *
 * @author mwakerman, bullman
 */
@Command(name = "atlasdb-perf", description = "The AtlasDB performance benchmark CLI.")
public class AtlasDbPerfCli {


    //================================================================================================================
    // CLI OPTIONS
    //================================================================================================================

    @Inject
    private HelpOption helpOption;

    @Arguments(description = "The performance benchmarks to run. Leave blank to run all performance benchmarks.")
    private static List<String> TESTS;


    @Option(name = {"-b", "--backend"}, description = "The underlying physical store to use e.g. 'POSTGRES'.")
    private static String BACKEND;

    @Option(name = {"-l", "--list-tests"}, description = "Lists all available tests.")
    private boolean LIST_TESTS;

    @Option(name = {"-o", "--output"}, description = "The file in which to store the test results. Leave blank to only write results to " +
                                                     "the console.")
    private static String OUTPUT_FILE;

    // Turn the backend into State that can be handed to the prepare() method of the benchmark classes.
    @State(Scope.Thread)
    public static class ThreadState {
        public volatile String backend = BACKEND;
    }

    public static void main(String[] args) throws Exception {
        AtlasDbPerfCli cli = SingleCommand.singleCommand(AtlasDbPerfCli.class).parse(args);
        if (cli.helpOption.showHelpIfRequested()) return;

        // If '--list-tests' is supplied, only print available tests.
        if (cli.LIST_TESTS) {
            listTests();
            return;
        }

        if (hasValidArgs(cli)) {
            run();
        } else {
            System.exit(1);
        }
    }

    private static void run() throws Exception {

        // TODO: explore the other options here?
        ChainedOptionsBuilder optBuilder = new OptionsBuilder().forks(1);

        if (TESTS == null) {
            // Do all tests.
            getAllTests().stream().forEach(clazz -> optBuilder.include(clazz.getSimpleName()));
        } else {
            TESTS.subList(1, TESTS.size()).stream().forEach(testName -> optBuilder.include(testName));
        }

        if (OUTPUT_FILE != null) {
            optBuilder.resultFormat(ResultFormatType.CSV);
            optBuilder.result(OUTPUT_FILE);
        }

        Options opt = optBuilder.build();
        Collection<RunResult> results = new Runner(opt).run();
        processResults(results);
    }

    /**
     *
     * @param cli
     */
    private static boolean hasValidArgs(AtlasDbPerfCli cli) {
        boolean isValid = true;

        if (cli.BACKEND == null) {
            System.err.println("Invalid arguments: must specify a --backend.");
            isValid = false;
        }

        return isValid;
    }

    /**
     * Prints all available performance benchmarks (one per line).
     */
    private static void listTests() {
        getAllTests().forEach(testClass ->
                System.out.println(testClass.getCanonicalName()));
    }

    /**
     * Scans the {@code com.palantir.atlasdb.performance.tests} package for all performance benchmark classes.
     *
     * @return a set of all performance benchmark classes.
     */
    private static Set<Class<?>> getAllTests() {
        // Note that we only allow the parent benchmark classes to be listed and this is the lowest
        // level of granularity provided with respect to running a subset of the benchmarks.
        Reflections reflections = new Reflections("com.palantir.atlasdb.performance.tests");
        return reflections.getTypesAnnotatedWith(BenchmarkMode.class).stream().filter(
                clazz -> !clazz.getCanonicalName().contains("generated")).collect(Collectors.toSet());
    }

    private static void processResults(Collection<RunResult> results) {
        for (RunResult result : results) {
            System.out.println("result.getPrimaryResult().getStatistics().getPercentile(2.5) = " + result.getPrimaryResult().getStatistics().getPercentile(2.5));
            System.out.println("result.getPrimaryResult().getStatistics().getPercentile(2.5) = " + result.getPrimaryResult().getStatistics().getPercentile(97.5));
        }
    }
}
