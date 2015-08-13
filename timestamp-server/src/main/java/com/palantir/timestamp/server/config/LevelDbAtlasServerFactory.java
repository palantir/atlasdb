package com.palantir.timestamp.server.config;

import java.io.File;
import java.io.IOException;

import org.iq80.leveldb.DBException;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.cleaner.CleanupFollower;
import com.palantir.atlasdb.cleaner.DefaultCleanerBuilder;
import com.palantir.atlasdb.keyvalue.TableMappingService;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.KVTableMappingService;
import com.palantir.atlasdb.keyvalue.impl.NamespaceMappingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.TableRemappingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.ValidatingQueryRewritingKeyValueService;
import com.palantir.atlasdb.keyvalue.leveldb.impl.LevelDbBoundStore;
import com.palantir.atlasdb.keyvalue.leveldb.impl.LevelDbKeyValueService;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SnapshotTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.common.base.Throwables;
import com.palantir.leader.PaxosLeaderElectionService;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.client.LockRefreshingLockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timestamp.PersistentTimestampService;
import com.palantir.timestamp.TimestampService;

public class LevelDbAtlasServerFactory implements AtlasDbServerFactory {
    final LevelDbKeyValueService rawKv;
    final KeyValueService kv;
    final RemoteLockService lock;
    final TimestampService ts;
    final SerializableTransactionManager txMgr;

    @Override
    public KeyValueService getKeyValueService() {
        return kv;
    }

    @Override
    public Supplier<TimestampService> getTimestampService() {
        return Suppliers.ofInstance(ts);
    }

    @Override
    public SerializableTransactionManager getTransactionManager() {
        return txMgr;
    }

    private LevelDbAtlasServerFactory(
                                      LevelDbKeyValueService rawKv,
                                      KeyValueService kv,
                                      RemoteLockService lock,
                                      TimestampService ts,
                                      SerializableTransactionManager txMgr) {
        this.rawKv = rawKv;
        this.kv = kv;
        this.lock = lock;
        this.ts = ts;
        this.txMgr = txMgr;
    }

    public static AtlasDbServerFactory create(PaxosLeaderElectionService leader, String dataDir, Schema schema) {
        LevelDbKeyValueService rawKv = createKv(dataDir);
        TimestampService ts = PersistentTimestampService.create(LevelDbBoundStore.create(rawKv));
        KeyValueService keyValueService = createTableMappingKv(rawKv, ts);

        schema.createTablesAndIndexes(keyValueService);
        SnapshotTransactionManager.createTables(keyValueService);

        TransactionService transactionService = TransactionServices.createTransactionService(keyValueService);
        RemoteLockService lock = LockRefreshingLockService.create(LockServiceImpl.create(new LockServerOptions() {
            private static final long serialVersionUID = 1L;

            @Override
            public boolean isStandaloneServer() {
                return false;
            }
        }));
        LockClient client = LockClient.of("single node leveldb instance");
        ConflictDetectionManager conflictManager = ConflictDetectionManagers.createDefault(keyValueService);
        SweepStrategyManager sweepStrategyManager = SweepStrategyManagers.createDefault(keyValueService);

        CleanupFollower follower = CleanupFollower.create(schema);
        Cleaner cleaner = new DefaultCleanerBuilder(keyValueService, lock, ts, client, ImmutableList.of(follower), transactionService).buildCleaner();
        SerializableTransactionManager ret = new SerializableTransactionManager(
                keyValueService,
                ts,
                client,
                lock,
                transactionService,
                Suppliers.ofInstance(AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS),
                conflictManager,
                sweepStrategyManager,
                cleaner);
        cleaner.start(ret);
        return new LevelDbAtlasServerFactory(rawKv, keyValueService, lock, ts, ret);
    }

    private static KeyValueService createTableMappingKv(KeyValueService kv, final TimestampService ts) {
            TableMappingService mapper = getMapper(ts, kv);
            kv = NamespaceMappingKeyValueService.create(TableRemappingKeyValueService.create(kv, mapper));
            kv = ValidatingQueryRewritingKeyValueService.create(kv);
            return kv;
    }

    private static LevelDbKeyValueService createKv(String dataDir) {
        try {
            return LevelDbKeyValueService.create(new File(dataDir));
        } catch (DBException | IOException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private static TableMappingService getMapper(final TimestampService ts, KeyValueService kv) {
        return KVTableMappingService.create(kv, new Supplier<Long>() {
            @Override
            public Long get() {
                return ts.getFreshTimestamp();
            }
        });
    }

}
