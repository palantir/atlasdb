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

package com.palantir.atlasdb.memory;

import java.util.Map;

import com.google.auto.service.AutoService;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.config.DbTimestampCreationSetting;
import com.palantir.atlasdb.config.DbTimestampCreationSettings;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampSeries;
import com.palantir.atlasdb.keyvalue.api.TimestampSeriesProvider;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.timestamp.DbTimeLockFactory;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.ManagedTimestampService;

@AutoService(DbTimeLockFactory.class)
public class InMemoryDbTimeLockFactory implements DbTimeLockFactory {
    private final Map<TimestampSeries, ManagedTimestampService> services = Maps.newHashMap();

    @Override
    public String getType() {
        return InMemoryAtlasDbConfig.TYPE;
    }

    @Override
    public KeyValueService createRawKeyValueService(MetricsManager metricsManager, KeyValueServiceConfig config,
            LeaderConfig leaderConfig) {
        return new InMemoryKeyValueService(true);
    }

    @Override
    public ManagedTimestampService createManagedTimestampService(KeyValueService rawKvs,
            DbTimestampCreationSetting dbTimestampCreationSetting, boolean initializeAsync) {
        return DbTimestampCreationSettings.caseOf(dbTimestampCreationSetting)
                .multipleSeries((unusedTableRef, series) ->
                        services.computeIfAbsent(series, unused -> new InMemoryTimestampService()))
                .singleSeries(unused -> new InMemoryTimestampService());
    }

    @Override
    public TimestampSeriesProvider createTimestampSeriesProvider(KeyValueService rawKvs, TableReference tableReference,
            boolean initializeAsync) {
        return services::keySet;
    }
}
