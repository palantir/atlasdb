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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.reflections.Reflections;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.performance.api.PerformanceTest;
import com.palantir.atlasdb.performance.api.PerformanceTestMetadata;
import com.palantir.atlasdb.performance.backend.PhysicalStore;

import io.airlift.airline.Command;
import io.airlift.airline.HelpOption;
import io.airlift.airline.Option;
import io.airlift.airline.SingleCommand;

/**
 * The Atlas Perf(ormance) CLI is a tool for making and running AtlasDB performance tests.
 *
 * @author mwakerman, bullman
 */
@Command(name = "atlasdb-perf", description = "The AtlasDB performance test CLI.")
public class AtlasDbPerfCLI {

    //================================================================================================================
    // CLI OPTIONS
    //================================================================================================================

    @Inject
    private HelpOption helpOption;

    @Option(name = {"-t", "--test"}, description = "The name of the performance test to run.")
    private String TEST_NAME;

    @Option(name = {"-b", "--backend"}, description = "The underlying physical store to use e.g. 'POSTGRES'.")
    private String BACKEND;

    @Option(name = {"-l", "--list-tests"}, description = "Lists all available tests.")
    private boolean LIST_TESTS;

    @Option(name = {"-o", "--output"}, description = "The file in which to store the test results. Leave  blank to only write results to " +
                                                     "the console.")
    private File OUTPUT_FILE;

    //================================================================================================================
    // MAIN & RUN METHODS
    //================================================================================================================

    public static void main(String[] args) throws Exception {
        AtlasDbPerfCLI cli = SingleCommand.singleCommand(AtlasDbPerfCLI.class).parse(args);
        if (cli.helpOption.showHelpIfRequested()) return;
        if (hasValidArguments(cli)) {
            cli.run();
        } else {
            System.exit(1);
        }
        // TODO (mwakerman): find the non-reaped thread rather than force an exit.
        System.exit(0);
    }

    private void run() throws Exception {

        // If '--list-tests' is supplied, only print available tests.
        if (LIST_TESTS) {
            listTests();
            return;
        }

        try (PhysicalStore physicalStore = PhysicalStore.create(PhysicalStore.Type.valueOf(BACKEND));
             KeyValueService kvs = physicalStore.connect()) {

            if (kvs == null) {
                System.err.println("Could not run performance test, unable to connect to KVS. Exiting.");
                System.exit(1);
            }

            PerformanceTest test = getPerformanceTest(TEST_NAME);
            test.setup(kvs);
            Stopwatch timer = Stopwatch.createStarted();
            test.run();
            timer.stop();
            // For now, just print the test duration.
            System.out.println(String.format("Test '%s': duration (millis): %d", TEST_NAME, timer.elapsed(TimeUnit.MILLISECONDS)));

            test.tearDown();

            if (OUTPUT_FILE != null) {
                // Always store dates in UTC.
                ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
                Path resultsFile = OUTPUT_FILE.toPath();
                Files.write(resultsFile,String.format("%s,%s,%s,%s\n", now, TEST_NAME, getTestVersion(test),
                        timer.elapsed(TimeUnit.MILLISECONDS)).getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);

            }
        }
    }


    //================================================================================================================
    // UTILITY METHODS
    //================================================================================================================

    /**
     * Validates the command line arguments.
     * @param cli the CLI being validated.
     * @return {@code true} if the command line arguments are valid, otherwise {@code false}
     */
    private static boolean hasValidArguments(AtlasDbPerfCLI cli) {
        boolean isValid = true;

        // Require both a test name and backend.
        if (cli.TEST_NAME != null ^ cli.BACKEND != null) {
            System.err.println("Invalid arguments: both a '--test' and a '--backend' argument are required.");
            isValid = false;
        }

        // Ensure the supplied test exists.
        if (cli.TEST_NAME != null) {
            boolean testExists = getAllTests().stream().filter(clazz ->
                    clazz.getAnnotation(PerformanceTestMetadata.class).name().equals(cli.TEST_NAME)).count() > 0;

            if (!testExists) {
                System.err.println("Invalid arguments: test '" + cli.TEST_NAME + "' does not exist.");
                isValid = false;
            }
        }

        // Validate the supplied backend.
        if (cli.BACKEND != null) {
            try {
                PhysicalStore.Type.valueOf(cli.BACKEND);
            } catch (IllegalArgumentException e) {
                System.err.println("Invalid arguments: backed '" + cli.BACKEND + "' does not exist.");
                isValid = false;
            }
        }

        return isValid;
    }

    /**
     * Prints all available performance tests (one per line).
     */
    private void listTests() {
        getAllTests().forEach(testClass ->
                System.out.println(testClass.getAnnotation(PerformanceTestMetadata.class).name()));
    }

    /**
     * Scans the {@code com.palantir.atlasdb.performance.tests} packed for all classes with
     * {@link PerformanceTestMetadata} annotations and returns those it finds.
     *
     * @return a set of all performance test classes.
     */
    private static Set<Class<?>> getAllTests() {
        Reflections reflections = new Reflections("com.palantir.atlasdb.performance.tests");
        return reflections.getTypesAnnotatedWith(PerformanceTestMetadata.class);
    }

    /**
     * Returns an instance of the performance test class for the specified test name.
     *
     * @param testName the {@link PerformanceTestMetadata} {@code name} of the performance test being instantiated.
     * @return an instance of the performance test class identified by the provided test name.
     * @throws IllegalAccessException if the performance test cannot be instantiated.
     * @throws InstantiationException if the performance test cannot be instantiated.
     */
    private static PerformanceTest getPerformanceTest(String testName)
            throws IllegalAccessException, InstantiationException {

        return (PerformanceTest) Iterables.getOnlyElement(getAllTests().stream()
                .filter(clazz -> clazz.getAnnotation(PerformanceTestMetadata.class).name().equals(testName))
                .collect(Collectors.toSet())).newInstance();
    }

    /**
     * Returns the {@link PerformanceTestMetadata#version()} annotation of the provided test.
     * @param test the test of which to return the version.
     * @return the version of the provided test.
     */
    private static int getTestVersion(PerformanceTest test) {
        return test.getClass().getAnnotation(PerformanceTestMetadata.class).version();
    }
}
