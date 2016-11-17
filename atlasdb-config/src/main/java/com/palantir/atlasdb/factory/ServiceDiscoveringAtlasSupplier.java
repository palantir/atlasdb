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
package com.palantir.atlasdb.factory;

import java.util.ServiceLoader;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.timestamp.DebugLogger;
import com.palantir.timestamp.TimestampService;
import com.palantir.util.debug.ThreadDumps;

public class ServiceDiscoveringAtlasSupplier {
    private static final ServiceLoader<AtlasDbFactory> loader = ServiceLoader.load(AtlasDbFactory.class);

    private static String timestampServiceCreationInfo = null;

    private final KeyValueServiceConfig config;
    private final Optional<LeaderConfig> leaderConfig;
    private final Supplier<KeyValueService> keyValueService;
    private final Supplier<TimestampService> timestampService;

    public ServiceDiscoveringAtlasSupplier(KeyValueServiceConfig config, Optional<LeaderConfig> leaderConfig) {
        this.config = config;
        this.leaderConfig = leaderConfig;

        AtlasDbFactory atlasFactory = StreamSupport.stream(loader.spliterator(), false)
                .filter(producesCorrectType())
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "No atlas provider for KeyValueService type " + config.type() + " could be found."
                        + " Have you annotated it with @AutoService(AtlasDbFactory.class)?"
                ));
        keyValueService = Suppliers.memoize(() -> atlasFactory.createRawKeyValueService(config, leaderConfig));
        timestampService = () -> atlasFactory.createTimestampService(getKeyValueService());
    }

    public KeyValueService getKeyValueService() {
        return keyValueService.get();
    }

    public TimestampService getTimestampService() {
        DebugLogger.logger.info("Fetching timestamp service from thread {}. This should only happen once.",
                Thread.currentThread().getName());

        String threadDump = ThreadDumps.programmaticThreadDump();
        if (timestampServiceCreationInfo == null) {
            timestampServiceCreationInfo = threadDump;
        } else {
            if (!leaderConfig.isPresent()) {
                DebugLogger.logger.error("Timestamp service fetched for a second time, and there is no leader config."
                        + "This means that you may soon encounter the MultipleRunningTimestampServices error."
                        + "Now outputting thread dumps from both fetches of the timestamp service...");
            } else {
                DebugLogger.logger.warn("Timestamp service fetched for a second time. This is only OK if you are "
                        + "running in an HA configuration and have just had a leadership election. "
                        + "You do have a leader config, but we're printing thread dumps from both fetches of the "
                        + "timestamp service, in case this second service was created in error.");
            }
            DebugLogger.logger.error("First thread dump: {}", timestampServiceCreationInfo);
            DebugLogger.logger.error("Second thread dump: {}", threadDump);
        }

        return timestampService.get();
    }

    private Predicate<AtlasDbFactory> producesCorrectType() {
        return factory -> config.type().equalsIgnoreCase(factory.getType());
    }
}
