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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.TransactionLockTimeoutException;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.StringLockDescriptor;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.assertj.core.api.ThrowableAssert;
import org.junit.Test;

public class AsyncTimelockServiceTransactionIntegrationTest extends AbstractAsyncTimelockServiceIntegrationTest {

    private static final TableReference TABLE = TableReference.create(Namespace.create("test"), "test");
    private static final byte[] DATA = "foo".getBytes();
    private static final Cell CELL = Cell.create("bar".getBytes(), "baz".getBytes());
    private static final String AGENT = "smith";

    private static final LockRequest EXCLUSIVE_ADVISORY_LOCK_REQUEST = LockRequest.builder(
                    ImmutableSortedMap.of(StringLockDescriptor.of("foo"), LockMode.WRITE))
            .build();

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final TransactionManager txnManager;

    public AsyncTimelockServiceTransactionIntegrationTest() {
        cluster.waitUntilLeaderIsElected(ImmutableList.of(AGENT));

        txnManager = TimeLockTestUtils.createTransactionManager(cluster, AGENT);
        txnManager.getKeyValueService().createTable(TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
    }

    @Test
    public void canExecuteWriteTransactions() {
        // write a value
        txnManager.runTaskWithRetry(txn -> {
            txn.put(TABLE, ImmutableMap.of(CELL, DATA));
            return null;
        });

        // read the value and write a new one
        byte[] retrievedData = txnManager.runTaskWithRetry(txn -> {
            byte[] existing = txn.get(TABLE, ImmutableSet.of(CELL)).get(CELL);
            txn.put(TABLE, ImmutableMap.of(CELL, DATA));
            return existing;
        });

        assertThat(retrievedData).isEqualTo(DATA);
    }

    @Test
    public void canCommitWritesWithExclusiveAdvisoryLocks() throws ExecutionException, InterruptedException {
        AtomicBoolean isExecuting = new AtomicBoolean(false);

        List<Future<?>> tasks = IntStream.range(0, 5)
                .mapToObj(i -> executor.submit((Callable<Void>) () ->
                        txnManager.runTaskWithLocksWithRetry(() -> EXCLUSIVE_ADVISORY_LOCK_REQUEST, (txn, locks) -> {
                            assertThat(isExecuting.compareAndSet(false, true)).isTrue();
                            Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(500L));
                            assertThat(isExecuting.compareAndSet(true, false)).isTrue();

                            txn.put(TABLE, ImmutableMap.of(CELL, DATA));
                            return null;
                        })))
                .collect(Collectors.toList());

        for (Future<?> task : tasks) {
            task.get();
        }
    }

    @Test
    public void advisoryLocksCanFail() throws ExecutionException, InterruptedException {
        ThrowableAssert.ThrowingCallable failingTxn =
                () -> txnManager.runTaskWithLocksWithRetry(() -> EXCLUSIVE_ADVISORY_LOCK_REQUEST, (txn, locks) -> {
                    LockRefreshToken token = locks.iterator().next().getLockRefreshToken();
                    txnManager.getLockService().unlock(token);
                    txn.put(TABLE, ImmutableMap.of(CELL, DATA));
                    return null;
                });

        assertThatThrownBy(failingTxn).isInstanceOf(TransactionLockTimeoutException.class);
    }
}
