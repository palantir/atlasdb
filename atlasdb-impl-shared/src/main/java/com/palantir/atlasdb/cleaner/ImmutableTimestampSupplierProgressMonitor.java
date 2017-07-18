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

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.palantir.lock.LockClient;

final class ImmutableTimestampSupplierProgressMonitor implements Supplier<Long> {
    private static final Logger log = LoggerFactory.getLogger(ImmutableTimestampSupplierProgressMonitor.class);

    private static final long TIME_TO_WARN_HR = 1L;
    private static final long TIME_TO_ERROR_HR = 24L;
    private static final String LONG_RUNNING_TRANSACTION_ERROR_MESSAGE = "Immutable timestamp has not been updated for"
            + " [{}] hour(s) for LockClient [{}]. This indicates to a very long running transaction.";

    private final Supplier<Long> supplier;

    private final List<ImmutableTimestampMonitor> monitors;

    private ImmutableTimestampSupplierProgressMonitor(Supplier<Long> supplier,
                                                      List<ImmutableTimestampMonitor> monitors) {
        this.supplier = supplier;
        this.monitors = monitors;
    }

    static Supplier<Long> createWithDefaultMonitoring(Supplier<Long> supplier, LockClient lockClient) {
        List<ImmutableTimestampMonitor> defaultMonitors = Lists.newArrayList();
        defaultMonitors.add(new ImmutableTimestampMonitor(TimeUnit.HOURS.toMillis(TIME_TO_WARN_HR),
                () -> log.warn(LONG_RUNNING_TRANSACTION_ERROR_MESSAGE, TIME_TO_WARN_HR, lockClient.getClientId())));

        defaultMonitors.add(new ImmutableTimestampMonitor(TimeUnit.HOURS.toMillis(TIME_TO_ERROR_HR),
                () -> log.error(LONG_RUNNING_TRANSACTION_ERROR_MESSAGE, TIME_TO_ERROR_HR, lockClient.getClientId())));

        return new ImmutableTimestampSupplierProgressMonitor(supplier, defaultMonitors);
    }

    @Override
    public Long get() {
        Long ts = supplier.get();
        for (ImmutableTimestampMonitor monitor : monitors) {
            monitor.update(ts);
        }
        return ts;
    }
}
