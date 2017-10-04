/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.timelock.paxos;

import java.util.Optional;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.factory.ServiceDiscoveringAtlasSupplier;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.timelock.paxos.DelegatingManagedTimestampService;
import com.palantir.atlasdb.timelock.paxos.ManagedTimestampService;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;

public class DbBoundTimestampCreator implements TimestampCreator {

    private KeyValueServiceConfig kvsConfig;

    public DbBoundTimestampCreator(KeyValueServiceConfig kvsConfig) {
        this.kvsConfig = kvsConfig;
    }

    @Override
    public Supplier<ManagedTimestampService> createTimestampService(String client, LeaderConfig leaderConfig) {
        ServiceDiscoveringAtlasSupplier atlasFactory = new ServiceDiscoveringAtlasSupplier(kvsConfig,
                Optional.of(leaderConfig),
                Optional.empty(),
                Optional.of(AtlasDbConstants.TIMELOCK_TIMESTAMP_TABLE));

        TimestampService timestampService = atlasFactory.getTimestampService();
        Preconditions.checkArgument(TimestampManagementService.class.isInstance(timestampService),
                "The timestamp service is not a managed timestamp service.");

        return () -> new DelegatingManagedTimestampService(timestampService,
                (TimestampManagementService) timestampService);
    }
}
