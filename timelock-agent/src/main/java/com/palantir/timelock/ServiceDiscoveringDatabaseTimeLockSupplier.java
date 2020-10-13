/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.config.DbTimestampCreationSetting;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.factory.AtlasDbServiceDiscovery;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.paxos.Client;
import com.palantir.timestamp.ManagedTimestampService;

/**
 * See {@link com.palantir.atlasdb.factory.ServiceDiscoveringAtlasSupplier}. This differs in that it encodes
 * DB-Timelock specific assumptions.
 */
public class ServiceDiscoveringDatabaseTimeLockSupplier implements AutoCloseable {
    private final Supplier<KeyValueService> keyValueService;
    private final Function<DbTimestampCreationSetting, ManagedTimestampService> timestampServiceFactory;

    public ServiceDiscoveringDatabaseTimeLockSupplier(
            MetricsManager metricsManager,
            KeyValueServiceConfig config,
            LeaderConfig leaderConfig) {
        AtlasDbFactory atlasFactory = AtlasDbServiceDiscovery.createAtlasFactoryOfCorrectType(config);
        keyValueService = Suppliers.memoize(
                () -> atlasFactory.createRawKeyValueService(
                        metricsManager,
                        config,
                        Optional::empty,
                        Optional.of(leaderConfig),
                        Optional.empty(), // This refers to an AtlasDB namespace - we use the config to talk to the db
                        AtlasDbFactory.THROWING_FRESH_TIMESTAMP_SOURCE, // This is how we give out timestamps!
                        AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC));
        timestampServiceFactory = creationSetting ->
                atlasFactory.createManagedTimestampService(
                        keyValueService.get(),
                        Optional.of(creationSetting),
                        AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
    }

    @Override
    public void close() {
        keyValueService.get().close();
    }

    public synchronized ManagedTimestampService getManagedTimestampService(DbTimestampCreationSetting setting) {
        return timestampServiceFactory.apply(setting);
    }
}
