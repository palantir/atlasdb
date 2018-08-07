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

package com.palantir.atlasdb.transaction.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.mockito.InOrder;

import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cleaner.api.Cleaner;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.timelock.hackweek.SynchronizedTransactionService;
import com.palantir.atlasdb.timelock.hackweek.JamesTransactionService;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;

public class SnapshotTransactionManagerTest {
    private static final String SETUP_TASK_METRIC_NAME =
            SnapshotTransactionManager.class.getCanonicalName() + ".setupTask";
    private static final String FINISH_TASK_METRIC_NAME =
            SnapshotTransactionManager.class.getCanonicalName() + ".finishTask";

    private final Cleaner cleaner = mock(Cleaner.class);
    private final KeyValueService keyValueService = mock(KeyValueService.class);
    private final JamesTransactionService james = new SynchronizedTransactionService();

    private final MetricsManager metricsManager = MetricsManagers.createForTests();
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private final SnapshotTransactionManager snapshotTransactionManager = new SnapshotTransactionManager(
            metricsManager,
            keyValueService,
            james,
            null,
            () -> AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS,
            null,
            null,
            cleaner,
            false,
            () -> AtlasDbConstants.DEFAULT_TRANSACTION_LOCK_ACQUIRE_TIMEOUT_MS,
            TransactionTestConstants.GET_RANGES_THREAD_POOL_SIZE,
            TransactionTestConstants.DEFAULT_GET_RANGES_CONCURRENCY,
            TimestampCache.createForTests(),
            MultiTableSweepQueueWriter.NO_OP,
            executorService);

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
    public void canCloseTransactionManagerWithNonCloseableLockService() {
        SnapshotTransactionManager newTransactionManager = new SnapshotTransactionManager(
                metricsManager,
                keyValueService,
                james,
                null,
                null,
                null,
                null,
                cleaner,
                false,
                () -> AtlasDbConstants.DEFAULT_TRANSACTION_LOCK_ACQUIRE_TIMEOUT_MS,
                TransactionTestConstants.GET_RANGES_THREAD_POOL_SIZE,
                TransactionTestConstants.DEFAULT_GET_RANGES_CONCURRENCY,
                TimestampCache.createForTests(),
                MultiTableSweepQueueWriter.NO_OP,
                executorService);
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
        snapshotTransactionManager.runTaskWithRetry(tx -> 42);
        MetricRegistry registry = snapshotTransactionManager.metricsManager.getRegistry();
        assertThat(registry.getNames())
                .contains(SETUP_TASK_METRIC_NAME)
                .contains(FINISH_TASK_METRIC_NAME);
        assertThat(registry.getTimers().get(SETUP_TASK_METRIC_NAME).getCount()).isGreaterThanOrEqualTo(1);
        assertThat(registry.getTimers().get(FINISH_TASK_METRIC_NAME).getCount()).isGreaterThanOrEqualTo(1);
    }
}
