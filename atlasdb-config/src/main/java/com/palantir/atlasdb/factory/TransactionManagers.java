/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.factory;

import java.util.ServiceLoader;
import java.util.Set;

import javax.net.ssl.SSLSocketFactory;

import com.google.common.base.Optional;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cleaner.Cleaner;
import com.palantir.atlasdb.cleaner.CleanupFollower;
import com.palantir.atlasdb.cleaner.DefaultCleanerBuilder;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.NamespacedKeyValueServices;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.Schemas;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.TransactionManager;
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

public class TransactionManagers {

    private static final ServiceLoader<AtlasDbFactory> loader = ServiceLoader.load(AtlasDbFactory.class);

    /**
     * Create a {@link SerializableTransactionManager} with provided configuration, {@link SSLSocketFactory}, {@link Schema},
     * and an environment in which to register HTTP server endpoints.
     */
    public static TransactionManager create(AtlasDbConfig config, Optional<SSLSocketFactory> sslSocketFactory, Schema schema, Environment env) {
        return create(config, sslSocketFactory, ImmutableSet.of(schema), env);
    }

    /**
     * Create a {@link SerializableTransactionManager} with provided configuration, {@link SSLSocketFactory}, a set of
     * {@link Schema}s, and an environment in which to register HTTP server endpoints.
     */
    public static SerializableTransactionManager create(AtlasDbConfig config,
                                            Optional<SSLSocketFactory> sslSocketFactory,
                                            Set<Schema> schemas,
                                            Environment env) {
        AtlasDbFactory kvsFactory = getKeyValueServiceFactory(config.keyValueService().type());
        KeyValueService kvs = NamespacedKeyValueServices.wrapWithStaticNamespaceMappingKvs(
                kvsFactory.createRawKeyValueService(config.keyValueService()));

        createLockAndTimestampServices(config, sslSocketFactory, env, kvsFactory.createTimestampService(kvs));

        RemoteLockService lock = initRemoteLockServices(sslSocketFactory, config.lock().servers());
        TimestampService timestamp = initRemoteTimeServices(sslSocketFactory, config.timestamp().servers());

        SnapshotTransactionManager.createTables(kvs);

        LockClient lockClient = LockClient.of("atlas instance");

        TransactionService transactionService = TransactionServices.createTransactionService(kvs);
        ConflictDetectionManager conflictManager = ConflictDetectionManagers.createDefault(kvs);
        SweepStrategyManager sweepStrategyManager = SweepStrategyManagers.createDefault(kvs);

        for (Schema schema : schemas) {
            Schemas.createTablesAndIndexes(schema, kvs);
        }

        CleanupFollower follower = CleanupFollower.create(schemas);

        Cleaner cleaner = new DefaultCleanerBuilder(kvs,
                lock,
                timestamp,
                lockClient,
                ImmutableList.of(follower),
                transactionService).buildCleaner();

        SerializableTransactionManager transactionManager = new SerializableTransactionManager(kvs,
                timestamp,
                lockClient,
                lock,
                transactionService,
                Suppliers.ofInstance(AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS),
                conflictManager,
                sweepStrategyManager,
                cleaner);

        return transactionManager;
    }

    private static AtlasDbFactory getKeyValueServiceFactory(String type) {
        for (AtlasDbFactory factory : loader) {
            if (factory.getType().equalsIgnoreCase(type)) {
                return factory;
            }
        }
        throw new IllegalStateException("No atlas provider for KeyValueService type " + type + " is on your classpath.");
    }

    private static TimestampService createLockAndTimestampServices(AtlasDbConfig config,
            Optional<SSLSocketFactory> sslSocketFactory, Environment env, final TimestampService ts) {
        TimestampService localTs;
        if (config.leader().isPresent()) {
            localTs = Leaders.createLockAndTimestampServices(config.leader().get(), sslSocketFactory, ts, env);
        } else {
            env.register(LockServiceImpl.create());
            env.register(ts);
            localTs = ts;
        }
        return localTs;
    }

    private static RemoteLockService initRemoteLockServices(
            Optional<SSLSocketFactory> sslSocketFactory, Set<String> uris) {
        return AtlasDbHttpClients.createProxyWithFailover(sslSocketFactory, uris, RemoteLockService.class);
    }

    private static TimestampService initRemoteTimeServices(
            Optional<SSLSocketFactory> sslSocketFactory, Set<String> uris) {
        return AtlasDbHttpClients.createProxyWithFailover(sslSocketFactory, uris, TimestampService.class);
    }

    public interface Environment {
        void register(Object resource);
    }
}
