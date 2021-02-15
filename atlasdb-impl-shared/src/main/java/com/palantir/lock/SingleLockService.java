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
package com.palantir.lock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:FinalClass") // Used for mocking
public class SingleLockService implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(SingleLockService.class);

    private final RemoteLockService lockService;
    private final String lockId;
    private final boolean isLockIdSafeForLogging;

    private LockRefreshToken token = null;

    private SingleLockService(LockService lockService, String lockId, boolean isLockIdSafeForLogging) {
        this.lockService = lockService;
        this.lockId = lockId;
        this.isLockIdSafeForLogging = isLockIdSafeForLogging;
    }

    public static SingleLockService createSingleLockService(LockService lockService, String lockId) {
        return new SingleLockService(lockService, lockId, false);
    }

    public static SingleLockService createSingleLockServiceWithSafeLockId(LockService lockService, String lockId) {
        return new SingleLockService(lockService, lockId, true);
    }

    public static SingleLockService createNamedLockServiceForTable(
            LockService lockService, String safePrefix, TableReference tableRef) {
        String lockId = StringUtils.trim(safePrefix) + " " + tableRef.getQualifiedName();
        return LoggingArgs.isSafe(tableRef)
                ? SingleLockService.createSingleLockServiceWithSafeLockId(lockService, lockId)
                : SingleLockService.createSingleLockService(lockService, lockId);
    }

    public void lockOrRefresh() throws InterruptedException {
        if (token != null) {
            Set<LockRefreshToken> refreshedTokens = lockService.refreshLockRefreshTokens(ImmutableList.of(token));
            log.info(
                    "Refreshed an existing lock token for {} in a single lock service (token {}); got {}",
                    getLockIdLoggingArg(),
                    SafeArg.of("existingLockToken", token),
                    SafeArg.of("refreshedTokens", refreshedTokens));
            if (refreshedTokens.isEmpty()) {
                token = null;
            }
        } else {
            LockDescriptor lock = StringLockDescriptor.of(lockId);
            LockRequest request = LockRequest.builder(ImmutableSortedMap.of(lock, LockMode.WRITE))
                    .doNotBlock()
                    .build();
            token = lockService.lock(LockClient.ANONYMOUS.getClientId(), request);
            log.info(
                    "Attempted to acquire the lock {} in a single lock service; got {}",
                    getLockIdLoggingArg(),
                    SafeArg.of("acquiredToken", token));
        }
    }

    public boolean haveLocks() {
        return token != null;
    }

    @Override
    public void close() {
        if (token != null) {
            lockService.unlock(token);
        }
    }

    private Arg<String> getLockIdLoggingArg() {
        return isLockIdSafeForLogging ? SafeArg.of("lockId", lockId) : UnsafeArg.of("lockId", lockId);
    }
}
