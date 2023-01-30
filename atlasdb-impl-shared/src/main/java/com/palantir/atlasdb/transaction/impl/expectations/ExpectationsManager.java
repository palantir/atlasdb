/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.expectations;

import com.palantir.atlasdb.transaction.impl.ExpectationsAwareTransaction;
import com.palantir.atlasdb.util.MetricsManager;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ExpectationsManager implements AutoCloseable {
    public static final long SCHEDULER_DELAY_MILLIS = Duration.ofMinutes(5).toMillis();

    private final Set<ExpectationsAwareTransaction> trackedTransactions = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean taskIsScheduled = new AtomicBoolean(false);
    private final ScheduledExecutorService executorService;
    private final GaugesForExpectationsAlertingMetrics metrics;

    private ExpectationsManager(
            ScheduledExecutorService executorService, GaugesForExpectationsAlertingMetrics metrics) {
        this.executorService = executorService;
        this.metrics = metrics;
    }

    private static ExpectationsManager createStarted(
            ScheduledExecutorService executorService, MetricsManager metricsManager) {
        ExpectationsManager manager =
                new ExpectationsManager(executorService, new GaugesForExpectationsAlertingMetrics(metricsManager));
        manager.scheduleExpectationsTask();
        return manager;
    }

    private void scheduleExpectationsTask() {
        if (!taskIsScheduled.compareAndExchange(false, true)) {
            executorService.scheduleWithFixedDelay(
                    new ExpectationsTask(trackedTransactions, metrics),
                    SCHEDULER_DELAY_MILLIS,
                    SCHEDULER_DELAY_MILLIS,
                    TimeUnit.MILLISECONDS);
        }
    }

    public void register(ExpectationsAwareTransaction transaction) {
        trackedTransactions.add(transaction);
    }

    public void unregister(ExpectationsAwareTransaction transaction) {
        trackedTransactions.remove(transaction);
    }

    public void markCompletion(ExpectationsAwareTransaction transaction) {
        try {
            unregister(transaction);
        } finally {
            transaction.runExpectationsCallbacks();
        }
    }

    @Override
    public void close() throws Exception {
        taskIsScheduled.set(true);
        executorService.shutdown();
    }
}
