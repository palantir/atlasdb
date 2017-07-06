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

import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.UUID;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

public class ExclusiveLock implements AsyncLock {

    @GuardedBy("this")
    private final Queue queue = new Queue();
    @GuardedBy("this")
    private UUID currentHolder = null;

    private final DelayedExecutor canceller;

    public ExclusiveLock(DelayedExecutor canceller) {
        this.canceller = canceller;
    }

    @Override
    public synchronized AsyncResult<Void> lock(UUID requestId, long deadlineMs) {
        return submit(new LockRequest(requestId, false), deadlineMs);
    }

    @Override
    public synchronized AsyncResult<Void> waitUntilAvailable(UUID requestId, long deadlineMs) {
        return submit(new LockRequest(requestId, true), deadlineMs);
    }

    @Override
    public synchronized void unlock(UUID requestId) {
        if (Objects.equals(requestId, currentHolder)) {
            currentHolder = null;
            processQueue();
        }
    }

    @VisibleForTesting
    synchronized UUID getCurrentHolder() {
        return currentHolder;
    }

    @GuardedBy("this")
    private AsyncResult<Void> submit(LockRequest request, long deadline) {
        queue.enqueue(request);
        processQueue();

        AsyncResult<Void> result = request.result;
        if (!result.isComplete()) {
            canceller.runAt(() -> cancel(request.requestId), deadline);
        }
        return result;
    }

    @GuardedBy("this")
    private synchronized void cancel(UUID requestId) {
        queue.timeoutAndRemove(requestId);
    }

    @GuardedBy("this")
    private void processQueue() {
        while (!queue.isEmpty() && currentHolder == null) {
            LockRequest head = queue.dequeue();

            if (!head.releaseImmediately) {
                currentHolder = head.requestId;
            }

            head.result.complete(null);
        }
    }

    private static class LockRequest {
        private final AsyncResult<Void> result = new AsyncResult<>();
        private final UUID requestId;
        private final boolean releaseImmediately;

        LockRequest(UUID requestId, boolean releaseImmediately) {
            this.requestId = requestId;
            this.releaseImmediately = releaseImmediately;
        }
    }

    @NotThreadSafe
    private static class Queue {

        private final LinkedHashMap<UUID, LockRequest> queue = Maps.newLinkedHashMap();

        public void enqueue(LockRequest request) {
            queue.put(request.requestId, request);
        }

        public boolean isEmpty() {
            return queue.isEmpty();
        }

        public LockRequest dequeue() {
            return queue.remove(queue.keySet().iterator().next());
        }

        public void timeoutAndRemove(UUID requestId) {
            LockRequest request = queue.remove(requestId);
            if (request != null) {
                request.result.timeout();
            }
        }

    }

}
