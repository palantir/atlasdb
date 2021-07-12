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
package com.palantir.atlasdb.timelock;

import static com.palantir.atlasdb.timelock.AbstractAsyncTimelockServiceIntegrationTest.DEFAULT_SINGLE_SERVER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

public class TimeLockServerDownIntegrationTest {
    private static final TableReference TABLE = TableReference.create(Namespace.create("test"), "test");
    private static final byte[] DATA = "foo".getBytes();
    private static final Cell CELL = Cell.create("bar".getBytes(), "baz".getBytes());

    private static final TestableTimelockCluster CLUSTER =
            new TestableTimelockCluster("paxosSingleServer.ftl", DEFAULT_SINGLE_SERVER);

    @ClassRule
    public static final RuleChain ruleChain = CLUSTER.getRuleChain();

    @Test
    public void getsDependencyExceptionFromTransactionsWhenDown() throws ExecutionException {
        TransactionManager txnManager = TimeLockTestUtils.createTransactionManager(CLUSTER);
        txnManager.getKeyValueService().createTable(TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);

        // write a value
        txnManager.runTaskWithRetry(txn -> {
            txn.put(TABLE, ImmutableMap.of(CELL, DATA));
            return null;
        });

        // read the value
        byte[] retrievedData = txnManager.runTaskWithRetry(
                txn -> txn.get(TABLE, ImmutableSet.of(CELL)).get(CELL));

        assertThat(retrievedData).isEqualTo(DATA);

        takeDownTimeLock();

        // Try to get again
        assertThatThrownBy(() -> txnManager.runTaskWithRetry(
                        txn -> txn.get(TABLE, ImmutableSet.of(CELL)).get(CELL)))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(TimeoutException.class);
    }

    private static void takeDownTimeLock() throws ExecutionException {
        CLUSTER.killAndAwaitTermination(CLUSTER.servers());
    }
}
