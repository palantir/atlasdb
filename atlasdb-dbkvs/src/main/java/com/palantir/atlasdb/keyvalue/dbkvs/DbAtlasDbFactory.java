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
package com.palantir.atlasdb.keyvalue.dbkvs;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.timestamp.InDbTimestampBoundStore;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.refreshable.Refreshable;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.timestamp.PersistentTimestampServiceImpl;
import com.palantir.timestamp.TimestampBoundStore;
import com.palantir.timestamp.TimestampStoreInvalidator;
import java.util.Optional;
import java.util.function.LongSupplier;

@AutoService(AtlasDbFactory.class)
public class DbAtlasDbFactory implements AtlasDbFactory<KeyValueServiceConfig> {
    public static final String TYPE = "relational";
    private static final String EMPTY_TABLE_PREFIX = "";

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
     * @param initializeAsync initialize asynchronously
     * @return The requested KeyValueService instance
     */
    @Override
    public KeyValueService createRawKeyValueService(
            MetricsManager metricsManager,
            KeyValueServiceConfig config,
            Refreshable<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig,
            Optional<LeaderConfig> leaderConfig,
            Optional<String> namespace,
            LongSupplier unusedLongSupplier,
            boolean initializeAsync) {
        Preconditions.checkArgument(
                config instanceof DbKeyValueServiceConfig,
                "DbAtlasDbFactory expects a configuration of type DbKeyValueServiceConfiguration, found %s",
                config.getClass());
        return ConnectionManagerAwareDbKvs.create((DbKeyValueServiceConfig) config, runtimeConfig, initializeAsync);
    }

    @Override
    public ManagedTimestampService createManagedTimestampService(
            KeyValueService rawKvs, Optional<TableReference> tableReferenceOverride, boolean initializeAsync) {
        Preconditions.checkArgument(
                !tableReferenceOverride
                        .map(AtlasDbConstants.DB_TIMELOCK_TIMESTAMP_TABLE::equals)
                        .orElse(false),
                "Cannot specify the DB TimeLock timestamp table as a timestamp table override!");
        Preconditions.checkArgument(
                rawKvs instanceof ConnectionManagerAwareDbKvs,
                "DbAtlasDbFactory expects a raw kvs of type ConnectionManagerAwareDbKvs, found %s",
                rawKvs.getClass());
        ConnectionManagerAwareDbKvs dbkvs = (ConnectionManagerAwareDbKvs) rawKvs;

        return PersistentTimestampServiceImpl.create(
                createTimestampBoundStore(tableReferenceOverride, dbkvs, initializeAsync), initializeAsync);
    }

    private static TimestampBoundStore createTimestampBoundStore(
            Optional<TableReference> tableRef, ConnectionManagerAwareDbKvs dbkvs, boolean initializeAsync) {
        // Not using the table prefix here, as the tableRef should contain any necessary prefix.
        return tableRef.map(reference ->
                        InDbTimestampBoundStore.create(dbkvs.getConnectionManager(), reference, initializeAsync))
                .orElseGet(() -> defaultTimestampBoundStore(dbkvs, initializeAsync));
    }

    private static TimestampBoundStore defaultTimestampBoundStore(
            ConnectionManagerAwareDbKvs dbkvs, boolean initializeAsync) {
        return InDbTimestampBoundStore.create(
                dbkvs.getConnectionManager(), defaultTimestampTable(), defaultTablePrefix(dbkvs), initializeAsync);
    }

    @Override
    public TimestampStoreInvalidator createTimestampStoreInvalidator(
            KeyValueService rawKvs, Optional<TableReference> tableReference) {
        ConnectionManagerAwareDbKvs dbkvs = (ConnectionManagerAwareDbKvs) rawKvs;
        return timestampStoreInvalidator(dbkvs, tableReference);
    }

    private TimestampStoreInvalidator timestampStoreInvalidator(
            ConnectionManagerAwareDbKvs dbKvs, Optional<TableReference> tableReference) {
        return tableReference
                .map(ref -> DbTimestampStoreInvalidator.create(dbKvs, ref, EMPTY_TABLE_PREFIX))
                .orElseGet(() ->
                        DbTimestampStoreInvalidator.create(dbKvs, defaultTimestampTable(), defaultTablePrefix(dbKvs)));
    }

    private static TableReference defaultTimestampTable() {
        return AtlasDbConstants.TIMESTAMP_TABLE;
    }

    private static String defaultTablePrefix(ConnectionManagerAwareDbKvs dbKvs) {
        return dbKvs.getTablePrefix();
    }
}
