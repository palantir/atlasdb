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

package com.palantir.atlasdb.timelock.lock;

import java.util.Collection;
import java.util.UUID;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.logsafe.SafeArg;

public class LockAcquirer {

    private static final Logger log = LoggerFactory.getLogger(LockAcquirer.class);

    public AsyncResult<HeldLocks> acquireLocks(UUID requestId, OrderedLocks locks, long deadlineMs) {
        AsyncResult<Void> future = acquireAllLocks(locks, lock -> lock.lock(requestId, deadlineMs));
        registerCompletionHandlers(future, requestId, locks);

        return future.map(ignored -> new HeldLocks(locks.get(), requestId));
    }

    public AsyncResult<Void> waitForLocks(UUID requestId, OrderedLocks locks, long deadlineMs) {
        return acquireAllLocks(locks, lock -> lock.waitUntilAvailable(requestId, deadlineMs));
    }

    private AsyncResult<Void> acquireAllLocks(
            OrderedLocks locks,
            Function<AsyncLock, AsyncResult<Void>> lockFunction) {
        AsyncResult<Void> future = AsyncResult.completedResult();
        for (AsyncLock lock : locks.get()) {
            future = future.concatWith(() -> lockFunction.apply(lock));
        }
        return future;
    }

    private void registerCompletionHandlers(AsyncResult<Void> future, UUID requestId, OrderedLocks locks) {
        future.onError(error -> {
            log.warn("Error while acquiring locks", SafeArg.of("requestId", requestId), error);
            unlockAll(requestId, locks.get());
        });
        future.onTimeout(() -> {
            log.info("Lock request timed out", SafeArg.of("requestId", requestId));
            unlockAll(requestId, locks.get());
        });
    }

    private void unlockAll(UUID requestId, Collection<AsyncLock> locks) {
        try {
            for (AsyncLock lock : locks) {
                lock.unlock(requestId);
            }
        } catch (Throwable t) {
            log.error("Error while unlocking locks", SafeArg.of("requestId", requestId), t);
        }
    }

}
