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
     * Lock watch events tend to come in pairs - a lock and an unlock event. However, unlocks are asynchronous, and
     * thus we need to wait until we have received the unlock event before proceeding for deterministic testing
     * behaviour.
     */
    public static void awaitUnlock(TransactionManager txnManager) {
        LockWatchIntegrationTestUtilities.awaitLockWatches(txnManager, version -> version % 2 == 0);
    }

    /**
     * The lock watch manager registers watch events every five seconds - therefore, tables may not be watched
     * immediately after a Timelock leader election.
     */
    public static void awaitTableWatched(TransactionManager txnManager) {
        LockWatchIntegrationTestUtilities.awaitLockWatches(txnManager, version -> version > -1);
    }

    public static void awaitLockWatches(TransactionManager txnManager, Predicate<Long> versionPredicate) {
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

    public static LockWatchManagerInternal extractInternalLockWatchManager(TransactionManager txnManager) {
        return (LockWatchManagerInternal) txnManager.getLockWatchManager();
    }

    public static TransactionManager createTransactionManager(
            double validationProbability, TestableTimelockCluster timelockCluster) {
        return TimeLockTestUtils.createTransactionManager(
                        timelockCluster,
                        Namespace.DEFAULT_NAMESPACE.getName(),
                        AtlasDbRuntimeConfig.defaultRuntimeConfig(),
                        ImmutableAtlasDbConfig.builder()
                                .lockWatchCaching(LockWatchCachingConfig.builder()
                                        .validationProbability(validationProbability)
                                        .build()),
                        Optional.empty(),
                        createSchema())
                .transactionManager();
    }

    private static Schema createSchema() {
        Schema schema = new Schema("Table", TEST_PACKAGE, Namespace.DEFAULT_NAMESPACE);
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
