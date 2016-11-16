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
 */
package com.palantir.atlasdb.cli.command.timestamp;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.cli.runner.InMemoryTestRunner;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.services.AtlasDbServicesFactory;
import com.palantir.atlasdb.services.LockAndTimestampModule;
import com.palantir.atlasdb.services.ServicesConfigModule;
import com.palantir.atlasdb.services.test.DaggerTestAtlasDbServices;

import io.airlift.airline.Command;

public class TimestampMigrationCommandTest {
    private static AtlasDbServicesFactory moduleFactory;

    private static final String TIMESTAMP_MIGRATION_COMMAND_NAME
            = TimestampMigrationCommand.class.getAnnotation(Command.class).name();

    private static final String DEFAULT_CONFIG = "src/test/resources/cli_test_config.yml";
    private static final String[] DEFAULT_CLI_ARGS = getCommandForConfigPath(DEFAULT_CONFIG);

    @BeforeClass
    public static void setup() throws Exception {
        moduleFactory = new AtlasDbServicesFactory() {
            @Override
            public AtlasDbServices connect(ServicesConfigModule servicesConfigModule) {
                return DaggerTestAtlasDbServices.builder()
                        .servicesConfigModule(servicesConfigModule)
                        .lockAndTimestampModule(new LockAndTimestampModule()) // TODO Jkong to fill this in
                        .build();
            }
        };
    }

    private InMemoryTestRunner makeRunner(String... args) {
        return new InMemoryTestRunner(TimestampMigrationCommand.class, args);
    }

    @Test
    public void testFailsWithoutTimelockBlock() throws Exception {
        try (InMemoryTestRunner runner = makeRunner(DEFAULT_CLI_ARGS)) {
            runner.connect(moduleFactory);
            assertThatThrownBy(() -> runner.run(true, false)).isInstanceOf(RuntimeException.class);
        }
    }

    private static String[] getCommandForConfigPath(String configPath) {
        List<String> cliArgs = ImmutableList.of(
                "-c",
                configPath,
                "timestamp",
                TIMESTAMP_MIGRATION_COMMAND_NAME);
        return cliArgs.toArray(new String[cliArgs.size()]);
    }
}
