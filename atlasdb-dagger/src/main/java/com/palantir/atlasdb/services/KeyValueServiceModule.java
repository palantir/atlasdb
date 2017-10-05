/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.services;

import javax.inject.Named;
import javax.inject.Singleton;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.NamespacedKeyValueServices;
import com.palantir.atlasdb.keyvalue.impl.ProfilingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.SweepStatsKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.TracingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.ValidatingQueryRewritingKeyValueService;
import com.palantir.atlasdb.schema.SweepSchema;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.timestamp.TimestampService;

import dagger.Module;
import dagger.Provides;

@Module
public class KeyValueServiceModule {

    @Provides
    @Singleton
    @Named("kvs")
    public KeyValueService provideWrappedKeyValueService(@Named("rawKvs") KeyValueService rawKvs,
                                                         TimestampService tss,
                                                         ServicesConfig config) {
        KeyValueService kvs = NamespacedKeyValueServices.wrapWithStaticNamespaceMappingKvs(rawKvs,
                config.atlasDbConfig().initializeAsync());
        kvs = ProfilingKeyValueService.create(kvs,
                config.atlasDbConfig().getKvsSlowLogThresholdMillis());
        kvs = TracingKeyValueService.create(kvs);
        kvs = AtlasDbMetrics.instrument(KeyValueService.class, kvs);
        kvs = ValidatingQueryRewritingKeyValueService.create(kvs);
        kvs = SweepStatsKeyValueService.create(kvs, tss);
        TransactionTables.createTables(kvs);
        ImmutableSet<Schema> schemas =
                ImmutableSet.<Schema>builder()
                        .add(SweepSchema.INSTANCE.getLatestSchema())
                        .addAll(config.schemas()).build();
        for (Schema schema : schemas) {
            Schemas.createTablesAndIndexes(schema, kvs);
        }
        return kvs;
    }

    @Provides
    @Singleton
    public TransactionService provideTransactionService(@Named("kvs") KeyValueService kvs) {
        return TransactionServices.createTransactionService(kvs);
    }

    @Provides
    @Singleton
    public ConflictDetectionManager provideConflictDetectionManager(@Named("kvs") KeyValueService kvs) {
        return ConflictDetectionManagers.create(kvs);
    }

    @Provides
    @Singleton
    public SweepStrategyManager provideSweepStrategyManager(@Named("kvs") KeyValueService kvs) {
        return SweepStrategyManagers.createDefault(kvs);
    }

}
