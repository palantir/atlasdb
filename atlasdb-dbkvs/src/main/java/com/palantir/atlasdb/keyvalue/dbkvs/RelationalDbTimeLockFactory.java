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

package com.palantir.atlasdb.keyvalue.dbkvs;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.config.DbTimestampCreationSetting;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampSeries;
import com.palantir.atlasdb.keyvalue.api.TimestampSeriesProvider;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.timestamp.InDbTimestampBoundStore;
import com.palantir.atlasdb.keyvalue.dbkvs.timestamp.MultiSequenceTimestampSeriesProvider;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.timestamp.DbTimeLockFactory;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.refreshable.Refreshable;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.timestamp.PersistentTimestampServiceImpl;
import com.palantir.timestamp.TimestampBoundStore;
import java.util.Optional;

@AutoService(DbTimeLockFactory.class)
public class RelationalDbTimeLockFactory implements DbTimeLockFactory {
    public static final String TYPE = "relational";

    private final AtlasDbFactory delegate = new DbAtlasDbFactory();

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public KeyValueService createRawKeyValueService(
            MetricsManager metricsManager,
            KeyValueServiceConfig config,
            Refreshable<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig,
            LeaderConfig leaderConfig) {
        return delegate.createRawKeyValueService(
                metricsManager,
                config,
                runtimeConfig,
                Optional.of(leaderConfig),
                Optional.empty(), // This refers to an AtlasDB namespace - we use the config to talk to the db
                AtlasDbFactory.THROWING_FRESH_TIMESTAMP_SOURCE, // This is how we give out timestamps!
                AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
    }

    @Override
    public ManagedTimestampService createManagedTimestampService(
            KeyValueService rawKvs, DbTimestampCreationSetting dbTimestampCreationSetting, boolean initializeAsync) {
        Preconditions.checkArgument(
                rawKvs instanceof ConnectionManagerAwareDbKvs,
                "DbAtlasDbFactory expects a raw kvs of type ConnectionManagerAwareDbKvs, found %s",
                rawKvs.getClass());
        ConnectionManagerAwareDbKvs dbkvs = (ConnectionManagerAwareDbKvs) rawKvs;

        return PersistentTimestampServiceImpl.create(
                multiSeries(
                        dbkvs,
                        dbTimestampCreationSetting.tableReference(),
                        dbTimestampCreationSetting.timestampSeries(),
                        initializeAsync),
                initializeAsync);
    }

    @Override
    public TimestampSeriesProvider createTimestampSeriesProvider(
            KeyValueService rawKvs, TableReference tableReference, boolean initializeAsync) {
        return MultiSequenceTimestampSeriesProvider.create(rawKvs, tableReference, initializeAsync);
    }

    private static TimestampBoundStore multiSeries(
            ConnectionManagerAwareDbKvs dbkvs,
            TableReference tableRef,
            TimestampSeries series,
            boolean initializeAsync) {
        return InDbTimestampBoundStore.createForMultiSeries(
                dbkvs.getConnectionManager(), tableRef, series, initializeAsync);
    }
}
