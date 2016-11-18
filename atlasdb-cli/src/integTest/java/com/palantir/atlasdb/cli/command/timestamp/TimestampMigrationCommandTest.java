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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ObjectArrays;
import com.palantir.atlasdb.cli.runner.InMemoryTestRunner;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbConfigs;
import com.palantir.atlasdb.config.TimeLockClientConfig;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.services.AtlasDbServicesFactory;
import com.palantir.atlasdb.services.ServicesConfigModule;
import com.palantir.atlasdb.services.test.DaggerTestAtlasDbServices;
import com.palantir.atlasdb.timelock.TimeLockServer;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;

import io.airlift.airline.Command;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;

public class TimestampMigrationCommandTest {
    private static final String TIMESTAMP_MIGRATION_COMMAND_NAME
            = TimestampMigrationCommand.class.getAnnotation(Command.class).name();

    private static final String NO_TIMELOCK_CONFIG = ResourceHelpers.resourceFilePath("cliTestConfigNoTimelock.yml");
    private static final String[] NO_TIMELOCK_COMMAND = getCommandForConfigPath(NO_TIMELOCK_CONFIG);

    private static final String TIMELOCK_CONFIG = ResourceHelpers.resourceFilePath("cliTestConfigTimelock.yml");
    private static final String[] TIMELOCK_COMMAND = getCommandForConfigPath(TIMELOCK_CONFIG);

    private static final TimestampServicesProvider REMOTE_PROVIDER = getTimestampServicesProvider();
    private static final AtlasDbServicesFactory MODULE_FACTORY = new AtlasDbServicesFactory() {
        @Override
        public AtlasDbServices connect(ServicesConfigModule servicesConfigModule) {
            return DaggerTestAtlasDbServices.builder()
                    .servicesConfigModule(servicesConfigModule)
                    .build();
        }
    };

    @ClassRule
    public static final DropwizardAppRule<TimeLockServerConfiguration> APP = new DropwizardAppRule<>(
            TimeLockServer.class,
            ResourceHelpers.resourceFilePath("singleTestServer.yml"));

    @After
    public void tearDown() {
        resetProvider(REMOTE_PROVIDER);
    }

    @Test
    public void doesNotRunWithoutTimelockBlock() throws Exception {
        try (InMemoryTestRunner runner = makeRunner(NO_TIMELOCK_COMMAND)) {
            runner.connect(MODULE_FACTORY);
            assertThatThrownBy(() -> runner.run(true, false))
                    .isInstanceOf(RuntimeException.class)
                    .withFailMessage("CLI returned with nonzero exit code");
        }
    }

    @Test
    public void canMigrateTimestamps() throws Exception {
        try (InMemoryTestRunner runner = makeRunner(TIMELOCK_COMMAND)) {
            AtlasDbServices services = runner.connect(MODULE_FACTORY);
            TimestampServicesProvider internalProvider =
                    TimestampServicesProviders.getInternalProviderFromAtlasDbConfig(services.getAtlasDbConfig());
            long ts1 = internalProvider.timestampService().getFreshTimestamp();
            runner.run(true, false);
            long ts2 = services.getTimestampService().getFreshTimestamp();
            assertThat(ts1).isLessThan(ts2);
        }
    }

    @Test
    public void throwsIfRequestingTimestampsAfterInvalidationForInternalService() throws Exception {
        try (InMemoryTestRunner runner = makeRunner(TIMELOCK_COMMAND)) {
            AtlasDbServices services = runner.connect(MODULE_FACTORY);
            TimestampServicesProvider internalProvider =
                    TimestampServicesProviders.getInternalProviderFromAtlasDbConfig(services.getAtlasDbConfig());
            internalProvider.timestampService().getFreshTimestamp();
            runner.run(true, false);
            assertThatThrownBy(() -> internalProvider.timestampService().getFreshTimestamp())
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    public void canMigrateTimestampsInReverseDirection() throws Exception {
        try (InMemoryTestRunner runner = makeRunner(ObjectArrays.concat(TIMELOCK_COMMAND, "-r"))) {
            AtlasDbServices services = runner.connect(MODULE_FACTORY);
            long ts1 = services.getTimestampService().getFreshTimestamp();
            runner.run(true, false);
            TimestampServicesProvider internalProvider =
                    TimestampServicesProviders.getInternalProviderFromAtlasDbConfig(services.getAtlasDbConfig());
            long ts2 = internalProvider.timestampService().getFreshTimestamp();
            assertThat(ts1).isLessThan(ts2);
        }
    }

    @Test
    public void throwsIfRequestingTimestampsAfterInvalidationForRemoteService() throws Exception {
        try (InMemoryTestRunner runner = makeRunner(ObjectArrays.concat(TIMELOCK_COMMAND, "-r"))) {
            AtlasDbServices services = runner.connect(MODULE_FACTORY);
            services.getTimestampService().getFreshTimestamp();
            runner.run(true, false);
            assertThatThrownBy(() -> services.getTimestampService().getFreshTimestamp())
                    .hasMessageContaining(IllegalStateException.class.getName());
        }
    }

    private static TimestampServicesProvider getTimestampServicesProvider() {
        try {
            AtlasDbConfig atlasConfig = AtlasDbConfigs.load(new File(TIMELOCK_CONFIG));
            TimeLockClientConfig clientConfig = atlasConfig.timelock().get();
            return TimestampServicesProviders.createFromTimelockConfiguration(clientConfig);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private static InMemoryTestRunner makeRunner(String... args) {
        return new InMemoryTestRunner(TimestampMigrationCommand.class, args);
    }

    private static void resetProvider(TimestampServicesProvider provider) {
        provider.timestampAdminService().invalidateTimestamps();
        provider.timestampAdminService().fastForwardTimestamp(0L);
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
