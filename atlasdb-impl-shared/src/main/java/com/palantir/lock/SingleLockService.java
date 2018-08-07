/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.lock;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

@SuppressWarnings("checkstyle:FinalClass") // Used for mocking
public class SingleLockService implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(SingleLockService.class);

    private final String lockId;
    private final boolean isLockIdSafeForLogging;

    private SingleLockService(String lockId, boolean isLockIdSafeForLogging) {
        this.lockId = lockId;
        this.isLockIdSafeForLogging = isLockIdSafeForLogging;
    }

    public static SingleLockService createSingleLockService(String lockId) {
        return new SingleLockService(lockId, false);
    }

    public static SingleLockService createSingleLockServiceWithSafeLockId(String lockId) {
        return new SingleLockService(lockId, true);
    }

    public static SingleLockService createNamedLockServiceForTable(
            String safePrefix,
            TableReference tableRef) {
        String lockId = StringUtils.trim(safePrefix) + " " + tableRef.getQualifiedName();
        return LoggingArgs.isSafe(tableRef)
                ? SingleLockService.createSingleLockServiceWithSafeLockId(lockId)
                : SingleLockService.createSingleLockService(lockId);
    }

    public void lockOrRefresh() throws InterruptedException {
    }

    public boolean haveLocks() {
        return true;
    }

    @Override
    public void close() {
    }

    private Arg<String> getLockIdLoggingArg() {
        return isLockIdSafeForLogging ? SafeArg.of("lockId", lockId) : UnsafeArg.of("lockId", lockId);
    }
}
