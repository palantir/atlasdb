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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.timelock.util.LoggableIllegalStateException;
import com.palantir.lock.LockDescriptor;
import com.palantir.logsafe.SafeArg;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

public class ExclusiveLock implements AsyncLock {

    private final LockDescriptor descriptor;

    @GuardedBy("this")
    private final LockRequestQueue queue = new LockRequestQueue();
    @GuardedBy("this")
    private UUID currentHolder = null;

    public ExclusiveLock(LockDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    @Override
    public synchronized AsyncResult<Void> lock(UUID requestId) {
        return submit(new LockRequest(requestId, false));
    }

    @Override
    public synchronized AsyncResult<Void> waitUntilAvailable(UUID requestId) {
        return submit(new LockRequest(requestId, true));
    }

    @Override
    public synchronized void unlock(UUID requestId) {
        if (Objects.equals(requestId, currentHolder)) {
            currentHolder = null;
            processQueue();
        }
    }

    @Override
    public synchronized void timeout(UUID requestId) {
        queue.timeoutAndRemoveIfStillQueued(requestId);
    }

    @Override
    public LockDescriptor getDescriptor() {
        return descriptor;
    }

    @VisibleForTesting
    synchronized UUID getCurrentHolder() {
        return currentHolder;
    }

    @GuardedBy("this")
    private AsyncResult<Void> submit(LockRequest request) {
        queue.enqueue(request);
        processQueue();

        return request.result;
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
    private static class LockRequestQueue {

        @SuppressWarnings("checkstyle:illegaltype")
        private final LinkedHashMap<UUID, LockRequest> queue = Maps.newLinkedHashMap();

        public void enqueue(LockRequest request) {
            LockRequest existingRequest = queue.put(request.requestId, request);
            if (existingRequest != null) {
                queue.put(request.requestId, existingRequest);
                throw new LoggableIllegalStateException(
                        "Cannot enqueue the same request id twice.",
                        SafeArg.of("requestId", request.requestId));
            }
        }

        public boolean isEmpty() {
            return queue.isEmpty();
        }

        public LockRequest dequeue() {
            return queue.remove(queue.keySet().iterator().next());
        }

        public void timeoutAndRemoveIfStillQueued(UUID requestId) {
            LockRequest request = queue.remove(requestId);
            if (request != null) {
                request.result.timeout();
            }
        }
    }
}
