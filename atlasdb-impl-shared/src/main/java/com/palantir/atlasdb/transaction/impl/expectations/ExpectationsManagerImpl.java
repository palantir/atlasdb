/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.atlasdb.transaction.api.expectations.ExpectationsAwareTransaction;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ExpectationsManagerImpl implements ExpectationsManager {
    public static final long SCHEDULER_DELAY_MILLIS = Duration.ofMinutes(5).toMillis();
    private final Set<ExpectationsAwareTransaction> trackedTransactions = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean taskIsScheduled = new AtomicBoolean(false);
    private final ScheduledExecutorService executorService;

    private ExpectationsManagerImpl(ScheduledExecutorService executorService) {
        this.executorService = executorService;
    }

    public static ExpectationsManager createStarted(ScheduledExecutorService executorService) {
        ExpectationsManagerImpl manager = new ExpectationsManagerImpl(executorService);
        manager.scheduleExpectationsTask();
        return manager;
    }

    private void scheduleExpectationsTask() {
        if (!taskIsScheduled.compareAndExchange(false, true)) {
            executorService.scheduleWithFixedDelay(
                    new ExpectationsTask(trackedTransactions),
                    SCHEDULER_DELAY_MILLIS,
                    SCHEDULER_DELAY_MILLIS,
                    TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void register(ExpectationsAwareTransaction transaction) {
        trackedTransactions.add(transaction);
    }

    @Override
    public void unregister(ExpectationsAwareTransaction transaction) {
        trackedTransactions.remove(transaction);
    }

    @Override
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
