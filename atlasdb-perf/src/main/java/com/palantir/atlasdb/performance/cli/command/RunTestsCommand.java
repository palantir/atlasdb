/**
 * Copyright 2015 Palantir Technologies
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
 */
package com.palantir.atlasdb.performance.cli.command;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.performance.cli.backend.PhysicalStore;
import com.palantir.atlasdb.performance.cli.backend.PhysicalStoreType;
import com.palantir.atlasdb.performance.test.api.PerformanceTest;
import com.palantir.atlasdb.performance.test.api.ValueGenerator;
import com.palantir.atlasdb.performance.test.api.annotation.PerfTest;
import com.palantir.atlasdb.performance.test.generator.RandomValueGenerator;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

@Command(name = "run", description = "Run tests")
public class RunTestsCommand implements Callable<Integer> {

    @Arguments(title = "TESTS",
            description = "tests to run",
            required = false)
    private String tests;

    @Option(name = {"-b", "--backend"},
            title = "PHYSICAL STORE",
            description = "underyling physical store (e.g. POSTGRES)",
            required = true)
    private PhysicalStoreType type;

    private static final Set<Class<? extends PerformanceTest>> ALL_TESTS =
            ImmutableSet.<Class<? extends PerformanceTest>>builder()
                    .build();

    @Override
    public Integer call() throws Exception {
        KeyValueService kvs = PhysicalStore.create(type).connect();
        ValueGenerator gen = new RandomValueGenerator(ThreadLocalRandom.current());
        Set<Class<? extends PerformanceTest>> allTestsClasses = getAllTests();

        Set<Class<? extends PerformanceTest>> testsToRun = allTestsClasses.stream()
                .filter(test -> getTestArguments().contains(test.getAnnotation(PerfTest.class).name()))
                .collect(Collectors.toSet());
        if (testsToRun.size() == 0 || testsToRun.size() != getTestArguments().size()) {
            printPossibleTests(allTestsClasses);
            return 1;
        }

        testsToRun.stream().forEach(testClass -> runTest(testClass, kvs, gen));
        return 0;
    }

    private void printPossibleTests(Set<Class<? extends PerformanceTest>> allTestsClasses) {
        String requestedTestsString = Joiner.on(", ").join(getTestArguments());
        String possibleTestsString = Joiner.on(", ").join(
                allTestsClasses.stream()
                        .map(clazz -> clazz.getAnnotation(PerfTest.class).name())
                        .collect(Collectors.toSet()));
        if (requestedTestsString.isEmpty()) {
            System.out.println(String.format("Please select a test to run (%s).", possibleTestsString));
        } else {
            System.out.println(
                    String.format("Not all requested tests (%s) exist. Valid tests include %s.",
                            requestedTestsString,
                            possibleTestsString));
        }
    }

    private void runTest(Class<? extends PerformanceTest> testClass, KeyValueService kvs, ValueGenerator gen) {
        try {
            testClass.newInstance().run(kvs, gen);
        } catch (InstantiationException e) {
            throw new RuntimeException(testClass.getCanonicalName() + " cannot be instantiated (needs a no args constructor?).", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(testClass.getCanonicalName() + " cannot be instantiated.", e);
        }
    }

    private List<String> getTestArguments() {
        return Lists.newArrayList(tests.split("\\s*(,|\\s)\\s*"));
    }

    protected Set<Class<? extends PerformanceTest>> getAllTests() {
        return ALL_TESTS;
    }

}
