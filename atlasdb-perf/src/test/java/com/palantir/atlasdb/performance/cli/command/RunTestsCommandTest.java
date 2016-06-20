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

import java.util.Set;
import java.util.concurrent.Callable;

import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.performance.test.api.PerformanceTest;
import com.palantir.atlasdb.performance.test.api.ValueGenerator;
import com.palantir.atlasdb.performance.test.api.annotation.PerfTest;

import io.airlift.airline.Cli;
import io.airlift.airline.Command;
import io.airlift.airline.Help;

public class RunTestsCommandTest {

    @Test
    public void testRequiresBackend() {
        assertFailure(runTest(new String[] { "run" }));
    }

    @Test
    public void testConnectsToPostgresBackend() {
        assertSuccessful(runTest(new String[] { "run", "no-op-test", "-b", "POSTGRES" }));
    }

    private int runTest(String[] args) {
        Cli.CliBuilder<Callable> builder = Cli.<Callable>builder("test-atlasdb-perf-tool")
                .withDescription("test the perf framework")
                .withDefaultCommand(Help.class)
                .withCommands(Help.class, TestableRunTestsCommand.class);
        Cli<Callable> parser = builder.build();
        try {
            return (Integer) parser.parse(args).call();
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }

    private void assertSuccessful(int returnVal) {
        Preconditions.checkArgument(returnVal == 0, "CLI exited with non-zero exit code.");
    }

    private void assertFailure(int returnVal) {
        Preconditions.checkArgument(returnVal == 1, "CLI exited with exit code zero.");
    }

    @Command(name = "run", description = "Run tests")
    public static class TestableRunTestsCommand extends RunTestsCommand {
        @Override
        protected Set<Class<? extends PerformanceTest>> getAllTests() {
            return ImmutableSet.of(NoOpTest.class);
        }
    }

    @PerfTest(name = "no-op-test")
    static class NoOpTest implements PerformanceTest {
        @Override
        public void run(KeyValueService kvs, ValueGenerator gen) {
        }
    }
}
