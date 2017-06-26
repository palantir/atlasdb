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
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.logsafe.SafeArg;

public class LockAcquirer {

    private static final Logger log = LoggerFactory.getLogger(LockAcquirer.class);

    public CompletableFuture<HeldLocks> acquireLocks(UUID requestId, OrderedLocks locks) {
        CompletableFuture<Void> future = acquireAllLocks(locks, lock -> lock.lock(requestId));
        registerErrorHandler(future, requestId, locks);

        return future.thenApply(ignored -> new HeldLocks(locks.get(), requestId));
    }

    public CompletableFuture<Void> waitForLocks(UUID requestId, OrderedLocks locks) {
        return acquireAllLocks(locks, lock -> lock.waitUntilAvailable(requestId));
    }

    private CompletableFuture<Void> acquireAllLocks(
            OrderedLocks locks,
            Function<AsyncLock, CompletableFuture<Void>> lockFunction) {
        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
        for (AsyncLock lock : locks.get()) {
            future = future.thenCompose(ignored -> lockFunction.apply(lock));
        }
        return future;
    }

    private void registerErrorHandler(CompletableFuture<Void> future, UUID requestId, OrderedLocks locks) {
        future.exceptionally(error -> {
            log.warn("Error while acquiring locks", SafeArg.of("requestUd", requestId), error);
            unlockAll(requestId, locks.get());
            return null;
        });
    }

    private void unlockAll(UUID requestId, Collection<AsyncLock> locks) {
        for (AsyncLock lock : locks) {
            lock.unlock(requestId);
        }
    }

}
