/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.dbkvs.util;

import com.google.common.base.Suppliers;
import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.SimpleTimedSqlConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.SqlConnectionSupplier;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.nexus.db.pool.ConnectionManager;
import com.palantir.nexus.db.pool.ReentrantManagedConnectionSupplier;
import com.palantir.nexus.db.pool.config.MaskedValue;
import com.palantir.refreshable.Refreshable;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public final class SqlConnectionSuppliers {
    private SqlConnectionSuppliers() {
        // Utility
    }

    public static SqlConnectionSupplier createSimpleConnectionSupplier(
            ConnectionManager connectionManager,
            DbKeyValueServiceConfig config,
            Refreshable<Optional<KeyValueServiceRuntimeConfig>> runtimeConfig) {
        runtimeConfig.subscribe(
                newRuntimeConfig -> updateConnManagerConfig(connectionManager, config, newRuntimeConfig));
        Supplier<Boolean> isCloseTrackingEnabled = Suppliers.memoizeWithExpiration(
                () -> runtimeConfig
                        .get()
                        .flatMap(DbKeyValueServiceConfigs::tryCastToDbKeyValueServiceRuntimeConfig)
                        .map(DbKeyValueServiceRuntimeConfig::enableCloseTracking)
                        .orElse(false),
                1,
                TimeUnit.MINUTES);
        ReentrantManagedConnectionSupplier connSupplier = ReentrantManagedConnectionSupplier.create(
                connectionManager, () -> Boolean.TRUE.equals(isCloseTrackingEnabled.get()));
        return new SimpleTimedSqlConnectionSupplier(connSupplier);
    }

    private static void updateConnManagerConfig(
            ConnectionManager connManager,
            DbKeyValueServiceConfig config,
            Optional<KeyValueServiceRuntimeConfig> runtimeConfig) {
        MaskedValue password = runtimeConfig
                .flatMap(DbKeyValueServiceConfigs::tryCastToDbKeyValueServiceRuntimeConfig)
                .map(DbKeyValueServiceRuntimeConfig::getDbPassword)
                .orElseGet(() -> config.connection().getDbPassword());
        connManager.setPassword(password.unmasked());
    }
}
