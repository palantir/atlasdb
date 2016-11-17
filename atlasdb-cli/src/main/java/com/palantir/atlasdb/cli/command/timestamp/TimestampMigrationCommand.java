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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.TimeLockClientConfig;
import com.palantir.atlasdb.services.AtlasDbServices;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

@Command(name = "migrate",
        description = "Migrates the current timestamp from an internal Timestamp Service to a Timelock Server,"
                + "or from a Timelock Server to an internal Timestamp Service.")
public class TimestampMigrationCommand extends AbstractTimestampCommand {
    private static final Logger log = LoggerFactory.getLogger(TimestampMigrationCommand.class);

    @Option(name = {"-r", "--reverse"},
            type = OptionType.COMMAND,
            description = "Performs migration from the Timelock Server to an internal Timestamp Service,"
                    + "instead of from the internal service to a timelock server.")
    private boolean reverse;

    private TimestampServicesProvider sourceServicesProvider;
    private TimestampServicesProvider destinationServicesProvider;

    @Override
    protected boolean requireTimestamp() {
        return false;
    }

    @Override
    protected int executeTimestampCommand(AtlasDbServices services) {
        if (!isConfigurationValid(services.getAtlasDbConfig())) {
            log.error("Timestamp Migration CLI must be run with details of a timelock configuration.");
            return 1;
        }

        try {
            constructServices(services);
        } catch (IllegalStateException exception) {
            log.error("The internal timestamp service must be persistent to use the Timestamp Migration CLI.");
            return 1;
        }

        TimestampMigrator migrator = new TimestampMigrator(sourceServicesProvider, destinationServicesProvider);
        migrator.migrateTimestamps();
        log.info("Timestamp migration CLI complete.");
        return 0;
    }

    private void constructServices(AtlasDbServices services) {
        TimestampServicesProvider localServicesProvider = getLocalServicesProvider(services);
        TimestampServicesProvider remoteServicesProvider = getRemoteServicesProvider(services);

        if (reverse) {
            sourceServicesProvider = remoteServicesProvider;
            destinationServicesProvider = localServicesProvider;
        } else {
            sourceServicesProvider = localServicesProvider;
            destinationServicesProvider = remoteServicesProvider;
        }
    }

    private static TimestampServicesProvider getLocalServicesProvider(AtlasDbServices services) {
        return TimestampServicesProviders.getInternalProviderFromAtlasDbConfig(services.getAtlasDbConfig());
    }

    private static TimestampServicesProvider getRemoteServicesProvider(AtlasDbServices services) {
        TimeLockClientConfig timeLockClientConfig = services.getAtlasDbConfig().timelock().get();
        return TimestampServicesProviders.createFromTimelockConfiguration(timeLockClientConfig);
    }

    private boolean isConfigurationValid(AtlasDbConfig config) {
        return config.timelock().isPresent();
    }

    @Override
    public boolean isOnlineRunSupported() {
        return false;
    }
}
