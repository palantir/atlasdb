package com.palantir.atlasdb.spi;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.cleaner.CleanupFollower;
import com.palantir.atlasdb.cleaner.DefaultCleanerBuilder;
import com.palantir.atlasdb.client.FailoverFeignTarget;
import com.palantir.atlasdb.client.TextDelegateDecoder;
import com.palantir.atlasdb.keyvalue.TableMappingService;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.KVTableMappingService;
import com.palantir.atlasdb.keyvalue.impl.NamespaceMappingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.TableRemappingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.ValidatingQueryRewritingKeyValueService;
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
import com.palantir.lock.LockClient;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timestamp.TimestampService;
import com.palantir.timestamp.server.config.AtlasDbServerConfiguration;
import com.palantir.timestamp.server.config.AtlasDbServerState;

import feign.Client;
import feign.Feign;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.jaxrs.JAXRSContract;

@SuppressWarnings({"rawtypes", "unchecked"})
public class AtlasDbServerStateProvider {
    private final ServiceLoader<AtlasDbFactory> loader = ServiceLoader.load(AtlasDbFactory.class);

    public AtlasDbServerState create(AtlasDbServerConfiguration config) throws IOException {
        String serverType = config.serverType;
        Iterator<AtlasDbFactory> factories = loader.iterator();
        while (factories.hasNext()) {
            AtlasDbFactory factory = factories.next();
            if (factory.getType().equals(serverType)) {
                return create(factory, config);
            }
        }
        throw new IllegalStateException("No atlas provider for server type " +
                serverType + " is on your classpath.");
    }

    private AtlasDbServerState create(AtlasDbFactory factory,
                                      AtlasDbServerConfiguration config) throws IOException {
        KeyValueService rawKvs = factory.createRawKeyValueService(config.extraConfig);
        RemoteLockService leadingLock;
        if (config.lockClient.embedded) {
            leadingLock = LockServiceImpl.create();
        } else {
            leadingLock = getServiceWithFailover(config.lockClient.servers, RemoteLockService.class);
        }
        TimestampService leadingTs;
        if (config.timestampClient.embedded) {
            leadingTs = factory.createTimestampService(rawKvs);
        } else {
            leadingTs = getServiceWithFailover(config.timestampClient.servers, TimestampService.class);
        }
        return createInternal(factory, rawKvs, new Schema(), leadingTs, leadingLock);
    }

    private static <T> T getServiceWithFailover(List<String> uris, Class<T> type) {
        ObjectMapper mapper = new ObjectMapper();
        FailoverFeignTarget<T> failoverFeignTarget = new FailoverFeignTarget<T>(uris, type);
        Client client = failoverFeignTarget.wrapClient(new Client.Default(null, null));
        return Feign.builder()
                .decoder(new TextDelegateDecoder(new JacksonDecoder(mapper)))
                .encoder(new JacksonEncoder(mapper))
                .contract(new JAXRSContract())
                .client(client)
                .retryer(failoverFeignTarget)
                .target(failoverFeignTarget);
    }

    private static AtlasDbServerState createInternal(final AtlasDbFactory factory,
                                                     final KeyValueService rawKv,
                                                     Schema schema,
                                                     TimestampService leaderTs,
                                                     RemoteLockService leaderLock) {
        KeyValueService keyValueService = createTableMappingKv(rawKv, leaderTs);

        schema.createTablesAndIndexes(keyValueService);
        SnapshotTransactionManager.createTables(keyValueService);

        TransactionService transactionService = TransactionServices.createTransactionService(keyValueService);
        LockClient client = LockClient.of("atlas instance");
        ConflictDetectionManager conflictManager = ConflictDetectionManagers.createDefault(keyValueService);
        SweepStrategyManager sweepStrategyManager = SweepStrategyManagers.createDefault(keyValueService);

        CleanupFollower follower = CleanupFollower.create(schema);
        Cleaner cleaner = new DefaultCleanerBuilder(keyValueService, leaderLock, leaderTs, client, ImmutableList.of(follower), transactionService).buildCleaner();
        SerializableTransactionManager ret = new SerializableTransactionManager(
                keyValueService,
                leaderTs,
                client,
                leaderLock,
                transactionService,
                Suppliers.ofInstance(AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS),
                conflictManager,
                sweepStrategyManager,
                cleaner);
        cleaner.start(ret);

        Supplier<TimestampService> tsSupplier = new Supplier<TimestampService>() {
            @Override
            public TimestampService get() {
                return factory.createTimestampService(rawKv);
            }
        };
        return new AtlasDbServerState(keyValueService, tsSupplier, ret);
    }

    private static KeyValueService createTableMappingKv(KeyValueService kv, final TimestampService ts) {
        TableMappingService mapper = getMapper(ts, kv);
        kv = NamespaceMappingKeyValueService.create(TableRemappingKeyValueService.create(kv, mapper));
        kv = ValidatingQueryRewritingKeyValueService.create(kv);
        return kv;
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
