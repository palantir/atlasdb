package com.palantir.atlasdb.cli.services;

import java.util.Set;

import javax.inject.Named;
import javax.inject.Singleton;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.NamespacedKeyValueServices;
import com.palantir.atlasdb.keyvalue.impl.SweepStatsKeyValueService;
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
import com.palantir.timestamp.TimestampService;

import dagger.Module;
import dagger.Provides;

@Module(includes = { ConfigModule.class, RawKeyValueServiceModule.class, LockAndTimestampModule.class })
public class KeyValueServiceModule {

    @Provides
    @Singleton
    @Named("kvs")
    public KeyValueService provideWrappedKeyValueService(@Named("rawKvs") KeyValueService rawKvs, TimestampService tss, Set<Schema> schemas) {
        KeyValueService kvs = NamespacedKeyValueServices.wrapWithStaticNamespaceMappingKvs(rawKvs);
        kvs = new SweepStatsKeyValueService(kvs, tss);
        TransactionTables.createTables(kvs);

        for (Schema schema : ImmutableSet.<Schema>builder().add(SweepSchema.INSTANCE.getLatestSchema()).addAll(schemas).build()) {
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
        return ConflictDetectionManagers.createDefault(kvs);
    }

    @Provides
    @Singleton
    public SweepStrategyManager provideSweepStrategyManager(@Named("kvs") KeyValueService kvs) {
        return SweepStrategyManagers.createDefault(kvs);
    }

}
