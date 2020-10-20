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

package com.palantir.atlasdb.timestamp;

import com.palantir.atlasdb.config.DbTimestampCreationSetting;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampSeriesProvider;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.timestamp.ManagedTimestampService;

/**
 * See {@link com.palantir.atlasdb.spi.AtlasDbFactory}. A {@link DbTimeLockFactory} is an extension of an
 * AtlasDbFactory that is expected to make suitable decisions around certain parameters to create a raw key-value
 * service, and it also supports extracting the timestamp series known about by the underlying database.
 */
public interface DbTimeLockFactory {
    String getType();

    KeyValueService createRawKeyValueService(
            MetricsManager metricsManager, KeyValueServiceConfig config, LeaderConfig leaderConfig);

    ManagedTimestampService createManagedTimestampService(
            KeyValueService rawKvs, DbTimestampCreationSetting dbTimestampCreationSetting, boolean initializeAsync);

    TimestampSeriesProvider createTimestampSeriesProvider(
            KeyValueService rawKvs, TableReference tableReference, boolean initializeAsync);
}
