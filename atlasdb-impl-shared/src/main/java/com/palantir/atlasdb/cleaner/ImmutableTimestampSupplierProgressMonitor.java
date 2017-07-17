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

package com.palantir.atlasdb.cleaner;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.palantir.lock.LockClient;

final class ImmutableTimestampSupplierProgressMonitor implements Supplier<Long> {
    private static final Logger log = LoggerFactory.getLogger(ImmutableTimestampSupplierProgressMonitor.class);

    private static final long TIME_TO_WARN = 1L;
    private static final long TIME_TO_ERROR = 24L;
    private static final TimeUnit TIME_UNIT = TimeUnit.HOURS;
    private static final String LONG_RUNNING_TRANSACTION_ERROR_MESSAGE = "Immutable timestamp has not been updated for"
            + " [{}] hour(s) for LockClient [{}]. This indicates to a very long running transaction.";

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final Supplier<Long> supplier;

    private ImmutableTimestampSupplierProgressMonitor(Supplier<Long> supplier) {
        this.supplier = supplier;
    }

    static Supplier<Long> createWithDefaultMonitoring(Supplier<Long> supplier, LockClient lockClient) {
        ImmutableTimestampSupplierProgressMonitor monitor = new ImmutableTimestampSupplierProgressMonitor(supplier);
        monitor.addMonitoring(TIME_TO_WARN,
                () -> log.warn(LONG_RUNNING_TRANSACTION_ERROR_MESSAGE, TIME_TO_WARN, lockClient.getClientId()));
        monitor.addMonitoring(TIME_TO_ERROR,
                () -> log.error(LONG_RUNNING_TRANSACTION_ERROR_MESSAGE, TIME_TO_ERROR, lockClient.getClientId()));
        return monitor;
    }

    private void addMonitoring(long periodInHours, Runnable runIfFailed) {
        Runnable monitorRunnable = ImmutableTimestampProgressMonitorRunner.of(supplier, runIfFailed);
        scheduler.scheduleAtFixedRate(monitorRunnable, periodInHours, periodInHours, TIME_UNIT);
    }

    @Override
    public Long get() {
        return supplier.get();
    }
}
