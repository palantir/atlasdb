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
package com.palantir.atlasdb.cli.api;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.file.Paths;

import io.airlift.airline.Cli;

public class SingleBackendCliTests {

    public static final String SIMPLE_ROCKSDB_CONFIG_FILENAME = "simple_rocksdb_config.yml";

    public static Cli<SingleBackendCommand> build(Class<? extends SingleBackendCommand> cmd) throws Exception {
        Cli.CliBuilder<SingleBackendCommand> builder = Cli.<SingleBackendCommand>builder("tester")
                .withDescription("Testing " + cmd)
                .withDefaultCommand(cmd)
                .withCommands(cmd);
        return builder.build();
    }

    public static String captureStdOut(Runnable runnable) {
        return captureStdOut(runnable, false);
    }

    public static String captureStdOut(Runnable runnable, boolean singleLine) {
        PrintStream ps = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));
        runnable.run();
        System.setOut(ps);
        return singleLine ? baos.toString().replace("\n", " ").replace("\r", " ") : baos.toString();
    }

    public static String getConfigPath(String filename) throws URISyntaxException {
        return Paths.get(SingleBackendCliTests.class.getClassLoader().getResource(filename).toURI()).toString();
    }
}
