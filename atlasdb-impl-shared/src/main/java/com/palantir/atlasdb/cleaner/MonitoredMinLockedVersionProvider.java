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

import com.google.common.annotations.VisibleForTesting;
import com.palantir.lock.LockClient;
import com.palantir.lock.RemoteLockService;

class MonitoredMinLockedVersionProvider {
    private static final Logger log = LoggerFactory.getLogger(MonitoredMinLockedVersionProvider.class);

    private static final long TIME_TO_WARN = 1L;
    private static final long TIME_TO_ERROR = 24L;
    private static final TimeUnit TIME_UNIT = TimeUnit.HOURS;
    private static final String LONG_RUNNING_TRANSACTION_ERROR_MESSAGE = "Immutable timestamp has not been updated for [{}] hour(s) for LockClient [{}]."
            + " This indicates to a very long running transaction.";

    private final RemoteLockService lockService;
    private final LockClient lockClient;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);;

    MonitoredMinLockedVersionProvider(RemoteLockService lockService, LockClient lockClient) {
        this.lockService = lockService;
        this.lockClient = lockClient;
        this.setupMonitoring(TIME_TO_WARN, false);
        this.setupMonitoring(TIME_TO_ERROR, true);
    }

    @VisibleForTesting
    protected TimeUnit getTimeUnit() {
        return TIME_UNIT;
    }

    private void setupMonitoring(long periodInHours, boolean isLogAsError) {
        scheduler.scheduleAtFixedRate(new Runnable() {
            private Long lastLockedVersionId = null;
            @Override
            public void run() {
                try {
                    Long newLockedVersionId = getMinLockedVersion();
                    if (lastLockedVersionId != null && lastLockedVersionId.equals(newLockedVersionId)) {
                        if (isLogAsError) {
                            log.error(LONG_RUNNING_TRANSACTION_ERROR_MESSAGE, periodInHours, lockClient.getClientId());
                        } else {
                            log.warn(LONG_RUNNING_TRANSACTION_ERROR_MESSAGE, periodInHours, lockClient.getClientId());
                        }
                    }
                    lastLockedVersionId = newLockedVersionId;
                } catch (Exception e) {
                    log.error("Checking immutable timestamp failed", e);
                }
            }
        }, 0, periodInHours, getTimeUnit());

    }

    Long getMinLockedVersion() {
        return lockService.getMinLockedInVersionId(lockClient.getClientId());
    }
}
