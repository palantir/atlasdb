/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.factory;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.limiter.AtlasClientLimiter;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceManager;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.refreshable.Refreshable;
import java.util.Optional;
import java.util.function.LongSupplier;

public final class DefaultKeyValueServiceManager implements KeyValueServiceManager {

    private final MetricsManager metricsManager;
    private final LongSupplier timestampSupplier;

    public DefaultKeyValueServiceManager(MetricsManager metricsManager, LongSupplier timestampSupplier) {
        this.metricsManager = metricsManager;
        this.timestampSupplier = timestampSupplier;
    }

    @Override
    public KeyValueService getKeyValueService(
            KeyValueServiceConfig config,
            Refreshable<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig,
            String namespace,
            boolean initializeAsync,
            AtlasClientLimiter clientLimiter) {
        // TODO(jakubk): In order to meaningfully memoize things we need to know cluster names.
        AtlasDbFactory atlasFactory = AtlasDbServiceDiscovery.createAtlasFactoryOfCorrectType(config);
        return atlasFactory.createRawKeyValueService(
                metricsManager,
                config,
                runtimeConfig,
                Optional.of(namespace),
                timestampSupplier,
                initializeAsync,
                clientLimiter);
    }
}
