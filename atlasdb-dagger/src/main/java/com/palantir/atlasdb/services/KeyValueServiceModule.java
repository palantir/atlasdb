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
package com.palantir.atlasdb.services;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.config.SweepConfig;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.internalschema.TransactionSchemaManager;
import com.palantir.atlasdb.internalschema.persistence.CoordinationServices;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.ProfilingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.SweepStatsKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.TracingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.ValidatingQueryRewritingKeyValueService;
import com.palantir.atlasdb.logging.KvsProfilingLogger;
import com.palantir.atlasdb.schema.CompactSchema;
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
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.timestamp.TimestampService;
import dagger.Module;
import dagger.Provides;
import javax.inject.Named;
import javax.inject.Singleton;

@Module
public class KeyValueServiceModule {

    @Provides
    @Singleton
    @Named("kvs")
    public KeyValueService provideWrappedKeyValueService(@Named("rawKvs") KeyValueService rawKvs,
                                                         TimestampService tss,
                                                         ServicesConfig config,
                                                         MetricsManager metricsManager) {
        config.adapter().setTimestampService(tss);

        KvsProfilingLogger.setSlowLogThresholdMillis(config.atlasDbConfig().getKvsSlowLogThresholdMillis());
        KeyValueService kvs = ProfilingKeyValueService.create(rawKvs);

        kvs = TracingKeyValueService.create(kvs);
        kvs = AtlasDbMetrics.instrument(metricsManager.getRegistry(), KeyValueService.class, kvs);
        kvs = ValidatingQueryRewritingKeyValueService.create(kvs);

        SweepConfig sweepConfig = config.atlasDbRuntimeConfig().sweep();
        kvs = SweepStatsKeyValueService.create(
                kvs,
                tss,
                sweepConfig::writeThreshold,
                sweepConfig::writeSizeThreshold,
                () -> true);

        TransactionTables.createTables(kvs);
        ImmutableSet<Schema> schemas =
                ImmutableSet.<Schema>builder()
                        .add(SweepSchema.INSTANCE.getLatestSchema())
                        .add(CompactSchema.INSTANCE.getLatestSchema())
                        .addAll(config.schemas()).build();
        for (Schema schema : schemas) {
            Schemas.createTablesAndIndexes(schema, kvs);
        }
        return kvs;
    }

    @Provides
    @Singleton
    public TransactionService provideTransactionService(
            @Named("kvs") KeyValueService kvs,
            CoordinationService<InternalSchemaMetadata> coordinationService) {
        return TransactionServices.createTransactionService(kvs,
                new TransactionSchemaManager(coordinationService));
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

    @Provides
    @Singleton
    public CoordinationService<InternalSchemaMetadata> provideMetadataCoordinationService(
            @Named("kvs") KeyValueService kvs,
            TimestampService ts,
            ServicesConfig config,
            MetricsManager metricsManager) {
        return CoordinationServices.createDefault(kvs, ts, metricsManager, config.atlasDbConfig().initializeAsync());
    }

}
