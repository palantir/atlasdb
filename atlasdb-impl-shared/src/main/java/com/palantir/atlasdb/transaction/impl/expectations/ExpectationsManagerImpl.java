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
import com.palantir.logsafe.Preconditions;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ExpectationsManagerImpl implements ExpectationsManager {

    private final Set<ExpectationsAwareTransaction> trackedTransactions = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean taskIsScheduled = new AtomicBoolean(false);
    private final ScheduledExecutorService executorService;

    public ExpectationsManagerImpl(ScheduledExecutorService executorService) {
        this.executorService = executorService;
    }

    @Override
    public void scheduleExpectationsTask(long delayMillis) {
        Preconditions.checkArgument(
                delayMillis > 0, "Transactional expectations manager scheduler delay must be strictly positive.");
        if (!taskIsScheduled.compareAndExchange(false, true)) {
            executorService.scheduleWithFixedDelay(
                    new ExpectationsTask(trackedTransactions), delayMillis, delayMillis, TimeUnit.MILLISECONDS);
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
        transaction.runExpectationsCallbacks();
        unregister(transaction);
    }

    @Override
    public void close() throws Exception {
        taskIsScheduled.set(true);
        executorService.shutdown();
    }
}
