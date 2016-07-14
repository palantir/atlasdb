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

import static java.util.stream.StreamSupport.stream;

import static com.google.common.base.Suppliers.memoize;

import java.util.ServiceLoader;
import java.util.function.Predicate;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.timestamp.TimestampService;

public class ServiceDiscoveringAtlasSupplier {
    private static final ServiceLoader<AtlasDbFactory> loader = ServiceLoader.load(AtlasDbFactory.class);

    private final KeyValueServiceConfig config;
    private final Supplier<KeyValueService> keyValueService;
    private final Supplier<TimestampService> timestampService;

    // TODO take an Optional<LeaderConfig> ?
    public ServiceDiscoveringAtlasSupplier(KeyValueServiceConfig config, Optional<LeaderConfig> leaderConfig) {
        this.config = config;
        AtlasDbFactory atlasFactory = stream(loader.spliterator(), false)
                .filter(producesCorrectType())
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                    "No atlas provider for KeyValueService type " + config.type() + " could be found. " +
                            "Have you annotated it with @AutoService(AtlasDbFactory.class)?"
                ));
        keyValueService = memoize(() -> atlasFactory.createRawKeyValueService(config, leaderConfig.get()));
        timestampService = () -> atlasFactory.createTimestampService(getKeyValueService());
    }

    public KeyValueService getKeyValueService() {
        return keyValueService.get();
    }

    public TimestampService getTimestampService() {
        return timestampService.get();
    }

    private Predicate<AtlasDbFactory> producesCorrectType() {
        return factory -> config.type().equalsIgnoreCase(factory.getType());
    }
}
