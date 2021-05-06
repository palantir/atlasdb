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

import static com.palantir.atlasdb.timelock.AbstractAsyncTimelockServiceIntegrationTest.DEFAULT_SINGLE_SERVER;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.TableDefinition;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

public final class LockWatchValueIntegrationTest {
    private static final String TEST_PACKAGE = "package";
    private static final byte[] DATA = "foo".getBytes();
    private static final Cell CELL = Cell.create("bar".getBytes(), "baz".getBytes());
    private static final String TABLE = "table";
    private static final TableReference TABLE_REF = TableReference.create(Namespace.DEFAULT_NAMESPACE, TABLE);

    private static final TestableTimelockCluster CLUSTER =
            new TestableTimelockCluster("paxosSingleServer.ftl", DEFAULT_SINGLE_SERVER);

    @ClassRule
    public static final RuleChain ruleChain = CLUSTER.getRuleChain();

    @Test
    public void minimalTest() {
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
        TransactionManager txnManager = TimeLockTestUtils.createTransactionManager(
                        CLUSTER,
                        UUID.randomUUID().toString(),
                        AtlasDbRuntimeConfig.defaultRuntimeConfig(),
                        Optional.empty(),
                        schema)
                .transactionManager();

        txnManager.runTaskWithRetry(txn -> {
            txn.put(TABLE_REF, ImmutableMap.of(CELL, DATA));
            return null;
        });

        Map<Cell, byte[]> result = txnManager.runTaskWithRetry(txn -> txn.get(TABLE_REF, ImmutableSet.of(CELL)));
        Map<Cell, byte[]> result2 = txnManager.runTaskWithRetry(txn -> txn.get(TABLE_REF, ImmutableSet.of(CELL)));

        assertThat(result).containsEntry(CELL, DATA);
        assertThat(result).containsExactlyInAnyOrderEntriesOf(result2);
    }
}
