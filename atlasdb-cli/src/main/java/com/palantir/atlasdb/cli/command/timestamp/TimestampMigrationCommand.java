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

import java.util.stream.Collectors;

import javax.net.ssl.SSLSocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.TimeLockClientConfig;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.atlasdb.services.DaggerAtlasDbServices;
import com.palantir.atlasdb.services.ServicesConfigModule;
import com.palantir.timestamp.PersistentTimestampService;
import com.palantir.timestamp.TimestampAdministrationService;
import com.palantir.timestamp.TimestampService;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

@Command(name = "migrate",
        description = "Migrates the current timestamp from an internal Timestamp Service to a Timelock Server,"
                + "or from one Timelock Server to an internal Timestamp Service.")
public class TimestampMigrationCommand extends AbstractTimestampCommand {
    private static final Logger log = LoggerFactory.getLogger(TimestampMigrationCommand.class);

    @Option(name = {"-r", "--reverse"},
            type = OptionType.COMMAND,
            description = "Performs migration from the Timelock Server to an internal Timestamp Service,"
                    + "instead of from the internal service to a timelock server.")
    private boolean reverse;

    private PersistentTimestampService internalService;

    private TimestampService remoteTimestampService;
    private TimestampAdministrationService remoteAdministrationService;

    @Override
    protected boolean requireTimestamp() {
        return false;
    }

    @Override
    protected int executeTimestampCommand(AtlasDbServices services) {
        if (!timelockBlockPresent(services.getAtlasDbConfig())) {
            log.error("Timestamp Migration CLI must be run with details of a timelock configuration.");
            return 1;
        }

        try {
            setSourceAndDestinationServices(services);
        } catch (IllegalStateException exception) {
            log.error("The internal timestamp service must be persistent to use the Timestamp Migration CLI.");
            return 1;
        }

        long targetTimestamp;
        try {
            targetTimestamp = getTimestampToFastForwardTo();
        } catch (IllegalStateException exception) {
            log.error("The source timestamp service has been invalidated!");
            return 1;
        }

        migrateTimestamp(targetTimestamp);
        log.info("Timestamp migration CLI complete.");
        return 0;
    }

    private long getTimestampToFastForwardTo() {
        if (reverse) {
            return remoteTimestampService.getFreshTimestamp();
        }
        return internalService.getFreshTimestamp();
    }

    private void migrateTimestamp(long targetTimestamp) {
        if (reverse) {
            internalService.fastForwardTimestamp(targetTimestamp);
            remoteAdministrationService.invalidateTimestamps();
        } else {
            remoteAdministrationService.fastForwardTimestamp(targetTimestamp);
            internalService.invalidateTimestamps();
        }
    }

    public TimestampService getInternalTimestampService(AtlasDbConfig config) {
        ServicesConfigModule scm = ServicesConfigModule.create(
                ImmutableAtlasDbConfig.copyOf(config)
                .withTimelock(Optional.absent())
        );
        return DaggerAtlasDbServices.builder()
                .servicesConfigModule(scm)
                .build()
                .getTimestampService();
    }

    private void setSourceAndDestinationServices(AtlasDbServices services) {
        TimestampService internalTimestampService = getInternalTimestampService(services.getAtlasDbConfig());
        if (!(internalTimestampService instanceof PersistentTimestampService)) {
            throw new IllegalStateException("Migration requires the internal timestamp service to be persistent.");
        }
        internalService = (PersistentTimestampService) internalTimestampService;

        remoteTimestampService = getTimelockProxy(services.getAtlasDbConfig(), TimestampService.class);
        remoteAdministrationService = getTimelockProxy(services.getAtlasDbConfig(),
                TimestampAdministrationService.class);
    }

    private boolean timelockBlockPresent(AtlasDbConfig config) {
        return config.timelock().isPresent();
    }

    private <T> T getTimelockProxy(AtlasDbConfig config, Class<T> clazz) {
        TimeLockClientConfig timeLockClientConfig = config.timelock().get();
        return AtlasDbHttpClients.createProxyWithFailover(
                createSslSocketFactoryFromClientConfig(timeLockClientConfig),
                timeLockClientConfig.serverListConfig()
                        .servers()
                        .stream()
                        .map(server -> server + "/" + timeLockClientConfig.client())
                        .collect(Collectors.toList()),
                clazz);
    }

    @Override
    public boolean isOnlineRunSupported() {
        return false;
    }

    private static Optional<SSLSocketFactory> createSslSocketFactoryFromClientConfig(TimeLockClientConfig config) {
        return TransactionManagers.createSslSocketFactory(config.serverListConfig().sslConfiguration());
    }
}
