/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.common.exception.AtlasDbDependencyException;

public class TimeLockServerDownIntegrationTest {
    private static final TableReference TABLE = TableReference.create(Namespace.create("test"), "test");
    private static final byte[] DATA = "foo".getBytes();
    private static final Cell CELL = Cell.create("bar".getBytes(), "baz".getBytes());
    private static final String CLIENT = "client";

    private static final TestableTimelockCluster CLUSTER = new TestableTimelockCluster(
            "https://localhost",
            CLIENT,
            "paxosSingleServer.yml");

    @ClassRule
    public static final RuleChain ruleChain = CLUSTER.getRuleChain();

    @Test
    public void getsDependencyExceptionFromTransactionsWhenDown() {
        SerializableTransactionManager txnManager = TimeLockTestUtils.createTransactionManager(CLUSTER);
        txnManager.getKeyValueService().createTable(TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);

        // write a value
        txnManager.runTaskWithRetry(txn -> {
            txn.put(TABLE, ImmutableMap.of(CELL, DATA));
            return null;
        });

        // read the value
        byte[] retrievedData = txnManager.runTaskWithRetry(txn -> txn.get(TABLE, ImmutableSet.of(CELL)).get(CELL));

        assertThat(retrievedData).isEqualTo(DATA);

        takeDownTimeLock();

        // Try to get again
        assertThatThrownBy(() -> txnManager.runTaskWithRetry(txn -> txn.get(TABLE, ImmutableSet.of(CELL)).get(CELL)))
                .isExactlyInstanceOf(AtlasDbDependencyException.class);
    }

    private static void takeDownTimeLock() {
        CLUSTER.servers().forEach(TestableTimelockServer::kill);
    }

}
