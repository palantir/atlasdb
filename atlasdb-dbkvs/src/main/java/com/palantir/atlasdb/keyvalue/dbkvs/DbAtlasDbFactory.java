/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.dbkvs;

import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.timestamp.InDbTimestampBoundStore;
import com.palantir.atlasdb.qos.QosClient;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.timestamp.PersistentTimestampService;
import com.palantir.timestamp.PersistentTimestampServiceImpl;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;

@AutoService(AtlasDbFactory.class)
public class DbAtlasDbFactory implements AtlasDbFactory {
    private static final Logger log = LoggerFactory.getLogger(DbAtlasDbFactory.class);
    public static final String TYPE = "relational";

    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * Creates a ConnectionManagerAwareDbKvs.
     *
     * @param config Configuration file.
     * @param runtimeConfig unused.
     * @param leaderConfig unused.
     * @param namespace unused.
     * @param unusedLongSupplier unused.
     * @param initializeAsync unused. Async initialization has not been implemented and is not propagated.
     * @param unusedQosClient unused.
     * @return The requested KeyValueService instance
     */
    @Override
    public KeyValueService createRawKeyValueService(
            MetricsManager metricsManager,
            KeyValueServiceConfig config,
            Supplier<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig,
            Optional<LeaderConfig> leaderConfig,
            Optional<String> namespace,
            LongSupplier unusedLongSupplier,
            boolean initializeAsync,
            QosClient unusedQosClient) {
        if (initializeAsync) {
            log.warn("Asynchronous initialization not implemented, will initialize synchronousy.");
        }

        Preconditions.checkArgument(config instanceof DbKeyValueServiceConfig,
                "DbAtlasDbFactory expects a configuration of type DbKeyValueServiceConfiguration, found %s",
                config.getClass());
        return ConnectionManagerAwareDbKvs.create((DbKeyValueServiceConfig) config);
    }

    @Override
    public TimestampService createTimestampService(
            KeyValueService rawKvs,
            Optional<TableReference> timestampTable,
            boolean initializeAsync) {
        if (initializeAsync) {
            log.warn("Asynchronous initialization not implemented, will initialize synchronousy.");
        }

        Preconditions.checkArgument(rawKvs instanceof ConnectionManagerAwareDbKvs,
                "DbAtlasDbFactory expects a raw kvs of type ConnectionManagerAwareDbKvs, found %s", rawKvs.getClass());
        ConnectionManagerAwareDbKvs dbkvs = (ConnectionManagerAwareDbKvs) rawKvs;

        return PersistentTimestampServiceImpl.create(createTimestampBoundStore(timestampTable, dbkvs));
    }

    @Override
    public TimestampManagementService getTimestampManagementService(TimestampService timestampService) {
        Preconditions.checkArgument(timestampService instanceof PersistentTimestampService,
                "TimestampManagementService must be created based on result of createTimestampService call."
                        + "\nExpected a PersistentTimestampServiceImpl, got %s", timestampService.getClass());

        return (PersistentTimestampService) timestampService;
    }

    private InDbTimestampBoundStore createTimestampBoundStore(Optional<TableReference> timestampTable,
            ConnectionManagerAwareDbKvs dbkvs) {
        return timestampTable
                .map(tableReference -> InDbTimestampBoundStore.create(
                    dbkvs.getConnectionManager(),
                    tableReference
                    /* Not using the table prefix here, as the tableRef should contain any necessary prefix.*/
                    ))
                .orElseGet(() -> InDbTimestampBoundStore.create(
                        dbkvs.getConnectionManager(),
                        AtlasDbConstants.TIMESTAMP_TABLE,
                        dbkvs.getTablePrefix()));
    }
}
