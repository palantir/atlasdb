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
package com.palantir.atlasdb.performance.cli;

import com.palantir.atlasdb.performance.BenchmarkParam;
import com.palantir.atlasdb.performance.MinimalReportFormatForTest;
import com.palantir.atlasdb.performance.PerformanceResults;
import com.palantir.atlasdb.performance.backend.DatabasesContainer;
import com.palantir.atlasdb.performance.backend.DockerizedDatabase;
import com.palantir.atlasdb.performance.backend.DockerizedDatabaseUri;
import com.palantir.atlasdb.performance.backend.KeyValueServiceInstrumentation;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.HelpOption;
import io.airlift.airline.Option;
import io.airlift.airline.SingleCommand;
import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Atlas Perf(ormance) CLI is a tool for making and running AtlasDB performance tests.
 *
 * This requires you to have a docker-machine running and configured correctly.
 *
 * @author mwakerman, bullman
 */

@Command(name = "atlasdb-perf", description = "The AtlasDB performance benchmark CLI.")
public class AtlasDbPerfCli {
    private static final Logger log = LoggerFactory.getLogger(AtlasDbPerfCli.class);

    @Inject
    private HelpOption helpOption;

    @Arguments(description = "The performance benchmarks to run. Leave blank to run all performance benchmarks.")
    private Set<String> tests;

    @Option(name = {"-b", "--backend"}, description = "Backing KVS stores to use. (e.g. POSTGRES or CASSANDRA)"
            + " Defaults to all backends if not specified.")
    private Set<String> backends;

    @Option(name = {"--db-uri"}, description = "Docker uri (e.g. POSTGRES@[phost:pport] or CASSANDRA@[chost:cport])."
            + "This is an alterative to specifying the --backend options that starts the docker containers locally.")
    private List<String> dbUris;

    @Option(name = {"-l", "--list-tests"}, description = "Lists all available benchmarks.")
    private boolean listTests;

    @Option(name = {"-o", "--output"},
            description = "The file in which to store the test results. "
                    + "Leave blank to only write results to the console.")
    private String outputFile;

    @Option(name = {"--test-run"}, description = "Run a single iteration of the benchmarks for testing purposes.")
    private boolean testRun;

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
        if (cli.dbUris != null) {
            runJmh(cli, getDockerUris(cli));
        } else {
            Set<String> backends = cli.backends != null
                    ? cli.backends
                    : KeyValueServiceInstrumentation.getBackends();
            try (DatabasesContainer container = startupDatabase(backends)) {
                runJmh(cli,
                        container.getDockerizedDatabases()
                                .stream()
                                .map(DockerizedDatabase::getUri)
                                .collect(Collectors.toList()));
            }
        }
    }

    private static void runJmh(AtlasDbPerfCli cli, List<DockerizedDatabaseUri> uris) throws Exception {
        ChainedOptionsBuilder optBuilder = new OptionsBuilder()
                .forks(1)
                .measurementIterations(1)
                .timeUnit(TimeUnit.MICROSECONDS)
                .shouldFailOnError(true)
                .param(BenchmarkParam.URI.getKey(),
                        uris.stream()
                                .map(DockerizedDatabaseUri::toString)
                                .collect(Collectors.toList())
                                .toArray(new String[uris.size()]));

        if (cli.tests == null) {
            getAllBenchmarks().forEach(b -> optBuilder.include(".*" + b));
        } else {
            cli.tests.forEach(b -> optBuilder.include(".*" + b));
        }

        if (!cli.testRun) {
            runCli(cli, optBuilder);
        } else {
            runCliInTestMode(optBuilder);
        }
    }

    private static void runCli(AtlasDbPerfCli cli, ChainedOptionsBuilder optBuilder) throws Exception {
        optBuilder.warmupIterations(1)
                .mode(Mode.SampleTime);

        Collection<RunResult> results = new Runner(optBuilder.build()).run();

        if (cli.outputFile != null) {
            new PerformanceResults(results).writeToFile(new File(cli.outputFile));
        }
    }

    private static void runCliInTestMode(ChainedOptionsBuilder optBuilder) throws RunnerException {
        optBuilder.warmupIterations(0)
                .mode(Mode.SingleShotTime);
        try {
            new Runner(optBuilder.build(), MinimalReportFormatForTest.get()).run();
        } catch (RunnerException e) {
            log.error("Error running benchmark test run.", e);
            throw e;
        }
    }

    private static DatabasesContainer startupDatabase(Set<String> backends) {
        return DatabasesContainer.startup(
                backends.stream()
                        .map(KeyValueServiceInstrumentation::forDatabase)
                        .collect(Collectors.toList()));
    }

    private static List<DockerizedDatabaseUri> getDockerUris(AtlasDbPerfCli cli) {
        return cli.dbUris.stream()
                .map(DockerizedDatabaseUri::fromUriString)
                .collect(Collectors.toList());
    }

    private static boolean hasValidArgs(AtlasDbPerfCli cli) {
        if (cli.backends != null && cli.dbUris != null) {
            throw new SafeRuntimeException("Cannot specify both --backends and --db-uris");
        }
        if (cli.backends != null) {
            cli.backends.forEach(backend -> {
                if (isInvalidBackend(backend)) {
                    throw new RuntimeException("Invalid backend specified. Valid options: "
                            + KeyValueServiceInstrumentation.getBackends() + " You provided: " + backend);
                }
            });
        }
        if (cli.dbUris != null) {
            try {
                getDockerUris(cli);
            } catch (Exception e) {
                throw new SafeRuntimeException(
                        "Invalid dockerized database uri. Must be of the form [dbtype]@[host:port]");
            }
        }
        return true;
    }

    private static void listAllBenchmarks() {
        getAllBenchmarks().forEach(System.out::println);
    }

    private static Set<String> getAllBenchmarks() {
        Reflections reflections = new Reflections(
                "com.palantir.atlasdb.performance.benchmarks",
                new MethodAnnotationsScanner());
        return reflections.getMethodsAnnotatedWith(Benchmark.class).stream()
                .map(method -> method.getDeclaringClass().getSimpleName() + "." + method.getName())
                .collect(Collectors.toSet());
    }

    private static boolean isInvalidBackend(String backend) {
        return !KeyValueServiceInstrumentation.getBackends().contains(backend);
    }
}
