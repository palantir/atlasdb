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

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.config.DbTimestampCreationSetting;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.TimestampSeries;
import com.palantir.paxos.Client;
import com.palantir.timelock.ServiceDiscoveringDatabaseTimeLockSupplier;
import com.palantir.timestamp.ManagedTimestampService;
import java.util.function.Supplier;

public final class DbBoundTimestampCreator implements TimestampCreator {

    private final ServiceDiscoveringDatabaseTimeLockSupplier serviceDiscoveringDatabaseTimeLockSupplier;

    public DbBoundTimestampCreator(
            ServiceDiscoveringDatabaseTimeLockSupplier serviceDiscoveringDatabaseTimeLockSupplier) {
        this.serviceDiscoveringDatabaseTimeLockSupplier = serviceDiscoveringDatabaseTimeLockSupplier;
    }

    @Override
    public Supplier<ManagedTimestampService> createTimestampService(Client client, LeaderConfig leaderConfig) {
        return () -> serviceDiscoveringDatabaseTimeLockSupplier.getManagedTimestampService(
                getTimestampCreationParameters(client));
    }

    @Override
    public void close() {
        serviceDiscoveringDatabaseTimeLockSupplier.close();
    }

    @VisibleForTesting
    static DbTimestampCreationSetting getTimestampCreationParameters(Client client) {
        return DbTimestampCreationSetting.of(
                AtlasDbConstants.DB_TIMELOCK_TIMESTAMP_TABLE, TimestampSeries.of(client.value()));
    }
}
