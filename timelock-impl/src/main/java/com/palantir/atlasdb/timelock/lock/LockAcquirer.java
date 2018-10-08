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
package com.palantir.atlasdb.timelock.lock;

import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.palantir.logsafe.SafeArg;

public class LockAcquirer {

    private static final Logger log = LoggerFactory.getLogger(LockAcquirer.class);

    private final LockLog lockLog;
    private final ScheduledExecutorService timeoutExecutor;

    public LockAcquirer(LockLog lockLog,
            ScheduledExecutorService timeoutExecutor) {
        this.lockLog = lockLog;
        this.timeoutExecutor = timeoutExecutor;
    }

    public AsyncResult<HeldLocks> acquireLocks(UUID requestId, OrderedLocks locks, TimeLimit timeout) {
        return new Acquisition(requestId, locks, timeout, lock -> lock.lock(requestId)).execute()
                .map(ignored -> new HeldLocks(lockLog, locks.get(), requestId));
    }

    public AsyncResult<Void> waitForLocks(UUID requestId, OrderedLocks locks, TimeLimit timeout) {
        return new Acquisition(requestId, locks, timeout, lock -> lock.waitUntilAvailable(requestId))
                .execute();
    }

    private class Acquisition {
        private final UUID requestId;
        private final OrderedLocks locks;
        private final TimeLimit timeout;
        private final Function<AsyncLock, AsyncResult<Void>> lockFunction;

        private AsyncResult<Void> result;

        Acquisition(
                UUID requestId,
                OrderedLocks locks,
                TimeLimit timeout,
                Function<AsyncLock, AsyncResult<Void>> lockFunction) {
            this.requestId = requestId;
            this.locks = locks;
            this.timeout = timeout;
            this.lockFunction = lockFunction;
        }

        public AsyncResult<Void> execute() {
            acquireLocks();
            registerCompletionHandlers();
            scheduleTimeout();

            return result;
        }

        private void acquireLocks() {
            try {
                AsyncResult<Void> lockResult = AsyncResult.completedResult();
                for (AsyncLock lock : locks.get()) {
                    lockResult = lockResult.concatWith(() -> lockFunction.apply(lock));
                }
                this.result = lockResult;
            } catch (Throwable t) {
                log.error("Error while acquiring locks");
                unlockAll();
                throw Throwables.propagate(t);
            }
        }

        private void registerCompletionHandlers() {
            result.onError(error -> {
                log.warn("Error while acquiring locks", SafeArg.of("requestId", requestId), error);
                unlockAll();
            });
            result.onTimeout(() -> {
                log.info("Lock request timed out", SafeArg.of("requestId", requestId));
                unlockAll();
            });
        }

        private void unlockAll() {
            try {
                for (AsyncLock lock : locks.get()) {
                    lock.unlock(requestId);
                }
            } catch (Throwable t) {
                log.error("Error while unlocking locks", SafeArg.of("requestId", requestId), t);
            }
        }

        private void scheduleTimeout() {
            if (result.isComplete()) {
                return;
            }

            timeoutExecutor.schedule(() -> timeoutAll(), timeout.getTimeMillis(), TimeUnit.MILLISECONDS);
        }

        private void timeoutAll() {
            for (AsyncLock lock : locks.get()) {
                lock.timeout(requestId);
            }
        }
    }

}
