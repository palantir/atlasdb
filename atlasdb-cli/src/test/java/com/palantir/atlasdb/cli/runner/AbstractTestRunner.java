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
package com.palantir.atlasdb.cli.runner;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.Callable;

import org.apache.commons.lang.ArrayUtils;

import com.palantir.atlasdb.cli.command.SingleBackendCommand;
import com.palantir.atlasdb.cli.services.AtlasDbServices;
import com.palantir.atlasdb.cli.services.AtlasDbServicesFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;

import io.airlift.airline.Cli;
import io.airlift.airline.Command;

public abstract class AbstractTestRunner <S extends AtlasDbServices> implements SingleBackendCliTestRunner {

    private final Class<? extends SingleBackendCommand> cmdClass;

    private String[] args;
    private SingleBackendCommand cmd;
    private S services;

    protected AbstractTestRunner(Class<? extends SingleBackendCommand> cmdClass, String... args) {
        this.cmdClass = cmdClass;
        this.args = args;
    }

    @Override
    public S connect(AtlasDbServicesFactory factory) throws Exception {
        cmd = buildCli(cmdClass).parse(buildArgs());
        services = cmd.connect(factory);
        return services;
    }

    private String[] buildArgs() throws URISyntaxException {
        String filePath = getResourcePath(getKvsConfigFileName());
        String cmdName = cmdClass.getAnnotation(Command.class).name();
        String[] initArgs = new String[] { cmdName, "-c", filePath };
        return (String[]) ArrayUtils.addAll(initArgs, args);
    }

    @Override
    public void parse(String... args) {
        this.args = Arrays.copyOf(args, args.length);
    }

    @Override
    public String run() {
        return run(false, true);
    }

    @Override
    public String run(boolean failOnNonZeroExit, boolean singleLine) {
        PrintStream ps = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));
        try {
            int ret = cmd.execute(services);
            if (ret != 0 && failOnNonZeroExit) {
                throw new RuntimeException("CLI returned with nonzero exit code");
            }
        } finally {
            System.setOut(ps);
        }
        return singleLine ? baos.toString().replace("\n", " ").replace("\r", " ") : baos.toString();
    }

    @Override
    public void close() throws Exception {
        if (services != null) {
            services.close();
        }
        cleanup(services.getAtlasDbConfig().keyValueService());
    }

    protected abstract String getKvsConfigFileName();

    protected abstract void cleanup(KeyValueServiceConfig kvsConfig);

    public static String getResourcePath(String fileName) throws URISyntaxException {
        return Paths.get(AbstractTestRunner.class.getClassLoader().getResource(fileName).toURI()).toString();
    }

    public static <T extends Callable<Integer>> Cli<T> buildCli(Class<T> cmd) {
        Cli.CliBuilder<T> builder = Cli.<T>builder("tester")
                .withDescription("Testing " + cmd)
                .withDefaultCommand(cmd)
                .withCommands(cmd);
        return builder.build();
    }

}
