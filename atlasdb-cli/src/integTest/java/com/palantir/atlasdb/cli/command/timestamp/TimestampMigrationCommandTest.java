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
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

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
    private static AtlasDbServicesFactory moduleFactory;
    private static TimestampServicesProvider remoteProvider;

    private static final String TIMESTAMP_MIGRATION_COMMAND_NAME
            = TimestampMigrationCommand.class.getAnnotation(Command.class).name();

    private static final String NO_TIMELOCK_CONFIG = "src/integTest/resources/cliTestConfigNoTimelock.yml";
    private static final String[] NO_TIMELOCK_COMMAND = getCommandForConfigPath(NO_TIMELOCK_CONFIG);

    private static final String TIMELOCK_CONFIG = "src/integTest/resources/cliTestConfigTimelock.yml";
    private static final String[] TIMELOCK_COMMAND = getCommandForConfigPath(TIMELOCK_CONFIG);

    @ClassRule
    public static final DropwizardAppRule<TimeLockServerConfiguration> APP = new DropwizardAppRule<>(
            TimeLockServer.class,
            ResourceHelpers.resourceFilePath("singleTestServer.yml"));

    @BeforeClass
    public static void setUpClass() throws Exception {
        moduleFactory = new AtlasDbServicesFactory() {
            @Override
            public AtlasDbServices connect(ServicesConfigModule servicesConfigModule) {
                return DaggerTestAtlasDbServices.builder()
                        .servicesConfigModule(servicesConfigModule)
                        .build();
            }
        };
        setUpTimestampServicesProvider();
    }

    private static void setUpTimestampServicesProvider() throws IOException {
        AtlasDbConfig atlasConfig = AtlasDbConfigs.load(new File(TIMELOCK_CONFIG));
        TimeLockClientConfig clientConfig = atlasConfig.timelock().get();
        remoteProvider = TimestampServicesProviders.createFromTimelockConfiguration(clientConfig);
    }

    private InMemoryTestRunner makeRunner(String... args) {
        return new InMemoryTestRunner(TimestampMigrationCommand.class, args);
    }

    @Test(expected = RuntimeException.class)
    public void doesNotRunWithoutTimelockBlock() throws Exception {
        try (InMemoryTestRunner runner = makeRunner(NO_TIMELOCK_COMMAND)) {
            runner.connect(moduleFactory);
            runner.run(true, false);
        }
    }

    @Test
    public void canMigrateTimestamps() throws Exception {
        try (InMemoryTestRunner runner = makeRunner(TIMELOCK_COMMAND)) {
            AtlasDbServices services = runner.connect(moduleFactory);
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
            AtlasDbServices services = runner.connect(moduleFactory);
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
            AtlasDbServices services = runner.connect(moduleFactory);
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
            AtlasDbServices services = runner.connect(moduleFactory);
            services.getTimestampService().getFreshTimestamp();
            runner.run(true, false);
            assertThatThrownBy(() -> services.getTimestampService().getFreshTimestamp())
                    .hasMessageContaining(IllegalStateException.class.getName());
        }
    }

    @After
    public void tearDown() {
        resetProvider(remoteProvider);
    }

    private static void resetProvider(TimestampServicesProvider provider) {
        provider.timestampAdministrationService().invalidateTimestamps();
        provider.timestampAdministrationService().fastForwardTimestamp(0L);
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
