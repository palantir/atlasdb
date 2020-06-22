/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.timelock.paxos;

import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.factory.ServiceDiscoveringAtlasSupplier;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.logsafe.Preconditions;
import com.palantir.paxos.Client;
import com.palantir.timestamp.DelegatingManagedTimestampService;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import java.util.Optional;
import java.util.function.Supplier;

public class DbBoundTimestampCreator implements TimestampCreator {

    private KeyValueServiceConfig kvsConfig;

    public DbBoundTimestampCreator(KeyValueServiceConfig kvsConfig) {
        this.kvsConfig = kvsConfig;
    }

    @Override
    public Supplier<ManagedTimestampService> createTimestampService(Client client, LeaderConfig leaderConfig) {
        ServiceDiscoveringAtlasSupplier atlasFactory = new ServiceDiscoveringAtlasSupplier(
                new MetricsManager(new MetricRegistry(), SharedTaggedMetricRegistries.getSingleton(), x -> false),
                kvsConfig,
                Optional::empty,
                Optional.of(leaderConfig),
                Optional.empty(),
                Optional.of(AtlasDbConstants.TIMELOCK_TIMESTAMP_TABLE),
                AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC,
                AtlasDbFactory.THROWING_FRESH_TIMESTAMP_SOURCE);

        TimestampService timestampService = atlasFactory.getManagedTimestampService();
        Preconditions.checkArgument(timestampService instanceof TimestampManagementService,
                "The timestamp service is not a managed timestamp service.");

        return () -> new DelegatingManagedTimestampService(timestampService,
                (TimestampManagementService) timestampService);
    }
}
