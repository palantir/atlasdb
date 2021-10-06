/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock;

import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.keyvalue.api.LockWatchCachingConfig;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerInternal;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.lock.watch.LockWatchVersion;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Predicate;
import org.awaitility.Awaitility;

public final class LockWatchIntegrationTestUtilities {
    private static final String TEST_PACKAGE = "package";
    private static final String TABLE = "table";

    private LockWatchIntegrationTestUtilities() {
        // no-op
    }

    /**
     * At commit time, transactions take out locks for all of their writes. However, these locks are released
     * asynchronously so as not to block execution of the transaction. Thus, we need to actively wait for unlock events,
     * and since each event increments the version by 1, we know that the version should be a multiple of two (assuming
     * no concurrent locks).
     */
    public static void awaitUnlock(TransactionManager txnManager) {
        LockWatchIntegrationTestUtilities.awaitUntilLockWatchVersionSatifies(txnManager, version -> version % 2 == 0);
    }

    /**
     * The lock watch manager registers watch events every five seconds - therefore, tables may not be watched
     * immediately after a TimeLock leader election.
     */
    public static void awaitTableWatched(TransactionManager txnManager) {
        LockWatchIntegrationTestUtilities.awaitUntilLockWatchVersionSatifies(txnManager, version -> version > -1);
    }

    /**
     * The internal version of the lock watch manager is hidden from the user, both to reduce API surface area, and
     * because certain classes aren't visible everywhere.
     */
    public static LockWatchManagerInternal extractInternalLockWatchManager(TransactionManager txnManager) {
        return (LockWatchManagerInternal) txnManager.getLockWatchManager();
    }

    public static TransactionManager createTransactionManager(
            double validationProbability, TestableTimelockCluster timelockCluster, String namespace) {
        return TimeLockTestUtils.createTransactionManager(
                        timelockCluster,
                        namespace,
                        AtlasDbRuntimeConfig.defaultRuntimeConfig(),
                        ImmutableAtlasDbConfig.builder()
                                .lockWatchCaching(LockWatchCachingConfig.builder()
                                        .validationProbability(validationProbability)
                                        .build()),
                        Optional.empty(),
                        createSchema())
                .transactionManager();
    }

    private static void awaitUntilLockWatchVersionSatifies(
            TransactionManager txnManager, Predicate<Long> versionPredicate) {
        LockWatchManagerInternal lockWatchManager = extractInternalLockWatchManager(txnManager);
        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .pollDelay(Duration.ofMillis(100))
                .until(() -> {
                    // Empty transaction will still get an update for lock watches
                    txnManager.runTaskThrowOnConflict(txn -> null);
                    return lockWatchManager
                            .getCache()
                            .getEventCache()
                            .lastKnownVersion()
                            .map(LockWatchVersion::version)
                            .filter(versionPredicate)
                            .isPresent();
                });
    }

    private static Schema createSchema() {
        Schema schema = new Schema("table", TEST_PACKAGE, Namespace.DEFAULT_NAMESPACE);
        TableDefinition tableDef = new TableDefinition() {
            {
                rowName();
                rowComponent("key", ValueType.BLOB);
                noColumns();
                enableCaching();
                conflictHandler(ConflictHandler.SERIALIZABLE_CELL);
            }
        };
        schema.addTableDefinition(TABLE, tableDef);
        return schema;
    }
}
