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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;

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

    @Inject
    private HelpOption helpOption;

    @Arguments(description = "The performance benchmarks to run. Leave blank to run all performance benchmarks.")
    private List<String> tests;

    @Option(name = {"-b", "--backend"}, description = "The underlying physical store to use e.g. 'POSTGRES'.")
    private String backend;

    @Option(name = {"-l", "--list-tests"}, description = "Lists all available benchmarks.")
    private boolean listTests;

    @Option(name = {"-o", "--output"}, description = "The file in which to store the test results. Leave blank to only write results to " +
                                                     "the console.")
    private String outputFile;

    public static void main(String[] args) throws Exception {
        AtlasDbPerfCli cli = SingleCommand.singleCommand(AtlasDbPerfCli.class).parse(args);

        if (cli.helpOption.showHelpIfRequested()) {
            return;
        }

        if (cli.listTests) {
            listAllBenchmarks();
            return;
        }

        if (hasValidArgs(cli)) {
            run(cli);
        } else {
            System.exit(1);
        }
    }

    private static void run(AtlasDbPerfCli cli) throws Exception {
        ChainedOptionsBuilder optBuilder = new OptionsBuilder()
                .forks(1)
                // the parameter name ("type") must match instance variable in KeyValueServiceConnector
                .param("type", cli.backend);

        if (cli.tests != null) {
            cli.tests.stream().forEach(testName -> optBuilder.include(testName));
        }

        if (cli.outputFile != null) {
            optBuilder.resultFormat(ResultFormatType.CSV);
            optBuilder.result(cli.outputFile);
        }

        new Runner(optBuilder.build()).run();
    }

    private static boolean hasValidArgs(AtlasDbPerfCli cli) {
        boolean isValid = true;

        if (cli.backend == null) {
            System.err.println("Invalid arguments: must specify a --backend.");
            isValid = false;
        }

        return isValid;
    }

    private static void listAllBenchmarks() {
        getAllBenchmarks().forEach(System.out::println);
    }

    private static Set<String> getAllBenchmarks() {
        Reflections reflections = new Reflections("com.palantir.atlasdb.performance.tests", new MethodAnnotationsScanner());
        return reflections.getMethodsAnnotatedWith(Benchmark.class).stream()
                .map(method -> method.getDeclaringClass().getSimpleName() + "." + method.getName())
                .collect(Collectors.toSet());
    }

}
