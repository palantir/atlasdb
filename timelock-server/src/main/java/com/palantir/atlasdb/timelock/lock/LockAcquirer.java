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

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockAcquirer {

    private static final Logger log = LoggerFactory.getLogger(LockAcquirer.class);

    public CompletableFuture<HeldLocks> acquireLocks(UUID requestId, List<AsyncLock> locks) {
        CompletableFuture<Void> future = acquireLocksInOrder(requestId, locks, false);
        registerErrorHandler(future, requestId, locks);

        return future.thenApply(ignored -> new HeldLocks(locks, requestId));
    }

    public CompletableFuture<Void> waitForLocks(UUID requestId, List<AsyncLock> locks) {
        return acquireLocksInOrder(requestId, locks, true);
    }

    private CompletableFuture<Void> acquireLocksInOrder(UUID requestId, List<AsyncLock> locks,
            boolean releaseImmediately) {
        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
        for (AsyncLock lock : locks) {
            future = future.thenCompose(ignored -> lock.lock(requestId));
            if (releaseImmediately) {
                future.thenRun(() -> lock.unlock(requestId));
            }
        }
        return future;
    }

    private void registerErrorHandler(CompletableFuture<Void> future, UUID requestId, List<AsyncLock> locks) {
        future.whenComplete((ignored, error) -> {
            if (error != null) {
                unlockAll(requestId, locks);
            }
        });
    }

    private void unlockAll(UUID requestId, List<AsyncLock> locks) {
        for (AsyncLock lock : locks) {
            lock.unlock(requestId);
        }
    }

}
