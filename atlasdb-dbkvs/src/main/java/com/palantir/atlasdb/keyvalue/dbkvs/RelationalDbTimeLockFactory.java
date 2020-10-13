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

import java.util.Optional;

import com.google.auto.service.AutoService;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.config.DbTimestampCreationSetting;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TimestampSeriesProvider;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.timestamp.DbTimeLockFactory;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.timestamp.ManagedTimestampService;

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
            LeaderConfig leaderConfig) {
        return delegate.createRawKeyValueService(
                metricsManager,
                config,
                Optional::empty,
                Optional.of(leaderConfig),
                Optional.empty(), // This refers to an AtlasDB namespace - we use the config to talk to the db
                AtlasDbFactory.THROWING_FRESH_TIMESTAMP_SOURCE, // This is how we give out timestamps!
                AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC);
    }

    @Override
    public ManagedTimestampService createManagedTimestampService(
            KeyValueService rawKvs,
            Optional<DbTimestampCreationSetting> dbTimestampCreationSetting,
            boolean initializeAsync) {
        return delegate.createManagedTimestampService(rawKvs, dbTimestampCreationSetting, initializeAsync);
    }

    @Override
    public TimestampSeriesProvider createTimestampSeriesProvider(KeyValueService rawKvs, boolean initializeAsync) {
        return () -> {
            throw new UnsupportedOperationException("Not supported yet!");
        };
    }
}
