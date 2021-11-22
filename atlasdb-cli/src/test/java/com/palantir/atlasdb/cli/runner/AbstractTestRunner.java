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
package com.palantir.atlasdb.cli.runner;

import com.palantir.atlasdb.cli.AtlasCli;
import com.palantir.atlasdb.cli.command.SingleBackendCommand;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.services.AtlasDbServicesFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import org.apache.commons.lang3.ArrayUtils;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.Callable;

public abstract class AbstractTestRunner implements SingleBackendCliTestRunner {

    private final Class<? extends SingleBackendCommand> cmdClass;

    private String[] args;
    private SingleBackendCommand cmd;
    private AtlasDbServices services;

    protected AbstractTestRunner(Class<? extends SingleBackendCommand> cmdClass, String... args) {
        this.cmdClass = cmdClass;
        this.args = args;
    }

    @Override
    public AtlasDbServices connect(AtlasDbServicesFactory factory) throws Exception {
        cmd = buildCommand(cmdClass, buildArgs());
        services = cmd.connect(factory);
        return services;
    }

    @Override
    public void freshCommand() throws URISyntaxException {
        cmd = buildCommand(cmdClass, buildArgs());
    }

    private String[] buildArgs() throws URISyntaxException {
        String filePath = getResourcePath(getKvsConfigFileName());
        String[] initArgs = new String[] {"-c", filePath};
        return (String[]) ArrayUtils.addAll(initArgs, args);
    }

    @Override
    public void parse(String... arguments) {
        this.args = Arrays.copyOf(arguments, arguments.length);
    }

    @Override
    public String run() {
        return run(false, true);
    }

    @Override
    public String run(boolean failOnNonZeroExit, boolean singleLine) {
        return StandardStreamUtilities.wrapSystemOut(
                () -> {
                    int ret = cmd.execute(services);
                    if (ret != 0 && failOnNonZeroExit) {
                        throw new SafeRuntimeException("CLI returned with nonzero exit code");
                    }
                },
                singleLine);
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
        return Paths.get(AbstractTestRunner.class
                        .getClassLoader()
                        .getResource(fileName)
                        .toURI())
                .toString();
    }

    public static <T extends Callable<Integer>> T buildCommand(Class<T> cmd, String... args) {
        return (T) AtlasCli.buildCli().parse(args);
    }
}
