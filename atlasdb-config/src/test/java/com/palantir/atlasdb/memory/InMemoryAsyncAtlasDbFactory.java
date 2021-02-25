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

package com.palantir.atlasdb.memory;

import com.google.auto.service.AutoService;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.AsyncInitializeableInMemoryKvs;
import com.palantir.atlasdb.keyvalue.impl.AsyncInitializeableInMemoryTimestampService;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.versions.AtlasDbVersion;
import com.palantir.refreshable.Refreshable;
import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.ManagedTimestampService;
import java.util.Optional;
import java.util.function.LongSupplier;

@AutoService(AtlasDbFactory.class)
public class InMemoryAsyncAtlasDbFactory implements AtlasDbFactory<KeyValueServiceConfig> {

    @Override
    public String getType() {
        return "memory-async";
    }

    @Override
    public KeyValueService createRawKeyValueService(
            MetricsManager unusedMetricsManager,
            KeyValueServiceConfig unusedConfig,
            Refreshable<Optional<KeyValueServiceRuntimeConfig>> unusedRuntimeConfig,
            Optional<LeaderConfig> unusedLeaderConfig,
            Optional<String> unused,
            LongSupplier unusedLongSupplier,
            boolean initializeAsync) {
        AtlasDbVersion.ensureVersionReported();
        return AsyncInitializeableInMemoryKvs.createAndStartInit(initializeAsync);
    }

    @Override
    public ManagedTimestampService createManagedTimestampService(
            KeyValueService rawKvs, Optional<TableReference> unused, boolean initializeAsync) {
        AtlasDbVersion.ensureVersionReported();
        if (initializeAsync) {
            return AsyncInitializeableInMemoryTimestampService.initializeWhenKvsIsReady(rawKvs);
        }
        return new InMemoryTimestampService();
    }
}
