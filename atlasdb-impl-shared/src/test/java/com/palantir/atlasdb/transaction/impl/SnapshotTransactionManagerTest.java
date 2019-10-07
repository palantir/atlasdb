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
package com.palantir.atlasdb.transaction.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.mockito.InOrder;

import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.cache.DefaultTimestampCache;
import com.palantir.atlasdb.cleaner.api.Cleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.transaction.ImmutableTransactionConfig;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.CloseableLockService;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockService;
import com.palantir.lock.impl.LegacyTimelockService;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timestamp.InMemoryTimestampService;

public class SnapshotTransactionManagerTest {
    private static final String SETUP_TASK_METRIC_NAME =
            SnapshotTransactionManager.class.getCanonicalName() + ".setupTask";
    private static final String FINISH_TASK_METRIC_NAME =
            SnapshotTransactionManager.class.getCanonicalName() + ".finishTask";

    private final CloseableLockService closeableLockService = mock(CloseableLockService.class);
    private final Cleaner cleaner = mock(Cleaner.class);
    private final KeyValueService keyValueService = mock(KeyValueService.class);

    private final MetricsManager metricsManager = MetricsManagers.createForTests();
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private final InMemoryTimestampService timestampService = new InMemoryTimestampService();
    private final SnapshotTransactionManager snapshotTransactionManager = new SnapshotTransactionManager(
            metricsManager,
            keyValueService,
            new LegacyTimelockService(timestampService, closeableLockService,
                    LockClient.of("lock")),
            timestampService,
            closeableLockService,
            mock(TransactionService.class),
            () -> AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS,
            null,
            null,
            cleaner,
            false,
            TransactionTestConstants.GET_RANGES_THREAD_POOL_SIZE,
            TransactionTestConstants.DEFAULT_GET_RANGES_CONCURRENCY,
            DefaultTimestampCache.createForTests(),
            MultiTableSweepQueueWriter.NO_OP,
            executorService,
            true,
            () -> ImmutableTransactionConfig.builder().build());

    @Test
    public void isAlwaysInitialized() {
        assertTrue(snapshotTransactionManager.isInitialized());
    }

    @Test
    public void closesKeyValueServiceOnClose() {
        snapshotTransactionManager.close();
        verify(keyValueService, times(1)).close();
    }

    @Test
    public void closesCleanerOnClose() {
        snapshotTransactionManager.close();
        verify(cleaner, times(1)).close();
    }

    @Test
    public void closesCloseableLockServiceOnClosingTransactionManager() throws IOException {
        snapshotTransactionManager.close();
        verify(closeableLockService, times(1)).close();
    }

    @Test
    public void canCloseTransactionManagerWithNonCloseableLockService() {
        InMemoryTimestampService ts = new InMemoryTimestampService();
        SnapshotTransactionManager newTransactionManager = new SnapshotTransactionManager(
                metricsManager,
                keyValueService,
                new LegacyTimelockService(ts, closeableLockService,
                        LockClient.of("lock")),
                ts,
                mock(LockService.class), // not closeable
                mock(TransactionService.class),
                null,
                null,
                null,
                cleaner,
                false,
                TransactionTestConstants.GET_RANGES_THREAD_POOL_SIZE,
                TransactionTestConstants.DEFAULT_GET_RANGES_CONCURRENCY,
                DefaultTimestampCache.createForTests(),
                MultiTableSweepQueueWriter.NO_OP,
                executorService,
                true,
                () -> ImmutableTransactionConfig.builder().build());
        newTransactionManager.close(); // should not throw
    }

    @Test
    public void cannotRegisterNullCallback() {
        assertThatThrownBy(() -> snapshotTransactionManager.registerClosingCallback(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    public void invokesCallbackOnClose() {
        Runnable callback = mock(Runnable.class);

        snapshotTransactionManager.registerClosingCallback(callback);
        verify(callback, never()).run();
        snapshotTransactionManager.close();
        verify(callback).run();
    }

    @Test
    public void invokesCallbacksInReverseOrderOfRegistration() {
        Runnable callback1 = mock(Runnable.class);
        Runnable callback2 = mock(Runnable.class);
        InOrder inOrder = inOrder(callback1, callback2);

        snapshotTransactionManager.registerClosingCallback(callback1);
        snapshotTransactionManager.registerClosingCallback(callback2);
        snapshotTransactionManager.close();
        inOrder.verify(callback2).run();
        inOrder.verify(callback1).run();
    }

    @Test
    public void registersMetrics() throws InterruptedException {
        when(closeableLockService.lock(any(), any())).thenReturn(new LockRefreshToken(BigInteger.ONE, Long.MAX_VALUE));
        snapshotTransactionManager.runTaskWithRetry(tx -> 42);
        MetricRegistry registry = snapshotTransactionManager.metricsManager.getRegistry();
        assertThat(registry.getNames())
                .contains(SETUP_TASK_METRIC_NAME)
                .contains(FINISH_TASK_METRIC_NAME);
        assertThat(registry.getTimers().get(SETUP_TASK_METRIC_NAME).getCount()).isGreaterThanOrEqualTo(1);
        assertThat(registry.getTimers().get(FINISH_TASK_METRIC_NAME).getCount()).isGreaterThanOrEqualTo(1);
    }

    @Test
    public void callsStartTransactionForReadOnlyTransactionsIfFlagIsSet() throws InterruptedException {
        TimelockService timelockService =
                spy(new LegacyTimelockService(timestampService, closeableLockService, LockClient.of("lock")));
        when(closeableLockService.lock(any(), any())).thenReturn(new LockRefreshToken(BigInteger.ONE, Long.MAX_VALUE));
        SnapshotTransactionManager transactionManager = createSnapshotTransactionManager(timelockService, true);

        transactionManager.runTaskReadOnly(tx -> "ignored");
        verify(timelockService).startIdentifiedAtlasDbTransaction();

        transactionManager.runTaskWithConditionReadOnly(PreCommitConditions.NO_OP, (tx, condition) -> "ignored");
        verify(timelockService, times(2)).startIdentifiedAtlasDbTransaction();
    }

    @Test
    public void doesNotCallStartTransactionForReadOnlyTransactionsIfFlagIsNotSet() {
        TimelockService timelockService =
                spy(new LegacyTimelockService(timestampService, closeableLockService, LockClient.of("lock")));
        SnapshotTransactionManager transactionManager = createSnapshotTransactionManager(timelockService, false);

        transactionManager.runTaskReadOnly(tx -> "ignored");
        transactionManager.runTaskWithConditionReadOnly(PreCommitConditions.NO_OP, (tx, condition) -> "ignored");
        verify(timelockService, never()).startIdentifiedAtlasDbTransaction();
    }

    private SnapshotTransactionManager createSnapshotTransactionManager(
            TimelockService timelockService, boolean grabImmutableTsLockOnReads) {
        return new SnapshotTransactionManager(
                metricsManager,
                keyValueService,
                timelockService,
                timestampService,
                mock(LockService.class), // not closeable
                mock(TransactionService.class),
                () -> null,
                null,
                null,
                cleaner,
                false,
                TransactionTestConstants.GET_RANGES_THREAD_POOL_SIZE,
                TransactionTestConstants.DEFAULT_GET_RANGES_CONCURRENCY,
                DefaultTimestampCache.createForTests(),
                MultiTableSweepQueueWriter.NO_OP,
                executorService,
                true,
                () -> ImmutableTransactionConfig.builder()
                        .lockImmutableTsOnReadOnlyTransactions(grabImmutableTsLockOnReads)
                        .build());
    }
}
