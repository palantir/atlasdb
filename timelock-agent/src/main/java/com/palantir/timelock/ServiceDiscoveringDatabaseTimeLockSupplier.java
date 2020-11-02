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

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.config.DbTimestampCreationSetting;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.factory.AtlasDbServiceDiscovery;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampSeriesProvider;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.timestamp.DbTimeLockFactory;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.timestamp.ManagedTimestampService;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * See {@link com.palantir.atlasdb.factory.ServiceDiscoveringAtlasSupplier}. This differs in that it encodes
 * DB-Timelock specific assumptions.
 */
public class ServiceDiscoveringDatabaseTimeLockSupplier implements AutoCloseable {
    private final Supplier<KeyValueService> keyValueService;
    private final Function<DbTimestampCreationSetting, ManagedTimestampService> timestampServiceFactory;
    private final Function<TableReference, TimestampSeriesProvider> timestampSeriesProvider;

    public ServiceDiscoveringDatabaseTimeLockSupplier(
            MetricsManager metricsManager, KeyValueServiceConfig config, LeaderConfig leaderConfig) {
        DbTimeLockFactory dbTimeLockFactory = AtlasDbServiceDiscovery.createDbTimeLockFactoryOfCorrectType(config);
        keyValueService = Suppliers.memoize(
                () -> dbTimeLockFactory.createRawKeyValueService(metricsManager, config, leaderConfig));
        timestampServiceFactory = creationSetting -> dbTimeLockFactory.createManagedTimestampService(
                keyValueService.get(), creationSetting, AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
        timestampSeriesProvider = tableRef -> dbTimeLockFactory.createTimestampSeriesProvider(
                keyValueService.get(), tableRef, AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
    }

    @Override
    public void close() {
        keyValueService.get().close();
    }

    public synchronized ManagedTimestampService getManagedTimestampService(DbTimestampCreationSetting setting) {
        Preconditions.checkState(
                setting.tableReference().equals(AtlasDbConstants.DB_TIMELOCK_TIMESTAMP_TABLE),
                "Attempted to create a managed timestamp service in db timelock that was not the normal db timelock"
                        + " timestamp table! This is unexpected, and we are prohibiting this creation for safety.",
                SafeArg.of("setting", setting));
        return timestampServiceFactory.apply(setting);
    }

    public synchronized TimestampSeriesProvider getTimestampSeriesProvider(TableReference tableReference) {
        Preconditions.checkState(
                tableReference.equals(AtlasDbConstants.DB_TIMELOCK_TIMESTAMP_TABLE),
                "Attempted to create a timestamp series provider in db timelock that was not the normal db timelock"
                        + " timestamp table! This is unexpected, and we are prohibiting this creation for safety.");
        return timestampSeriesProvider.apply(tableReference);
    }
}
