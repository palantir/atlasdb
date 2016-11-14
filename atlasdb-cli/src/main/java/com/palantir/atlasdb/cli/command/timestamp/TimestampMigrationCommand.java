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
import com.palantir.atlasdb.config.TimeLockClientConfig;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.timestamp.TimestampService;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

@Command(name = "timestamp-migration",
        description = "Migrates the current timestamp from an internal Timestamp Service to a Timelock Server,"
                + "or from one Timelock Server to an internal Timestamp Service.")
public class TimestampMigrationCommand extends AbstractTimestampCommand {
    private static final Logger log = LoggerFactory.getLogger(TimestampMigrationCommand.class);

    @Option(name = {"-r", "--reverse"},
            type = OptionType.COMMAND,
            description = "Performs migration from the Timelock Server to an internal Timestamp Service,"
                    + "instead of from the internal service to a timelock server.")
    private boolean reverse;

    private TimestampService sourceService;
    private TimestampService destinationService;

    @Override
    protected boolean requireTimestamp() {
        return false;
    }

    @Override
    protected int executeTimestampCommand(AtlasDbServices services) {
        if (!timelockBlockPresent(services.getAtlasDbConfig())) {
            log.error("Remote Migration CLI must be run with details of a timelock configuration.");
            return 1;
        }

        setSourceAndDestinationServices(services);
        long timestamp = sourceService.getFreshTimestamp();
        try {
            TimestampUtils.fastForwardTimestamp(destinationService, timestamp);
        } catch (IllegalStateException exception) {
            log.error("Attempted to fast forward a timestamp service which doesn't bear administrative capabilities.");
            return 1;
        }

        // TODO How to invalidate the old table?

        log.info("Timestamp migration cli complete.");
        return 0;
    }

    private void setSourceAndDestinationServices(AtlasDbServices services) {
        TimestampService internalService = services.getTimestampService();
        TimestampService remoteService = getTimelockProxy(services.getAtlasDbConfig(), TimestampService.class);

        if (reverse) {
            sourceService = remoteService;
            destinationService = internalService;
        } else {
            sourceService = internalService;
            destinationService = remoteService;
        }
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
