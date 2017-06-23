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

import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Queues;

public class ExclusiveLock implements AsyncLock {

    private static final Logger log = LoggerFactory.getLogger(ExclusiveLock.class);

    private final Queue<LockRequest> queue = Queues.newArrayDeque();
    private UUID currentHolder = null;

    @Override
    public synchronized CompletableFuture<Void> lock(UUID requestId) {
        LockRequest request = new LockRequest(requestId);
        queue.add(request);
        processQueue();

        return request.result;
    }

    @Override
    public synchronized void unlock(UUID requestId) {
        if (requestId.equals(currentHolder)) {
            currentHolder = null;
            processQueue();
        }
    }

    @VisibleForTesting
    synchronized UUID getCurrentHolder() {
        return currentHolder;
    }

    private void processQueue() {
        if (currentHolder != null || queue.isEmpty()) {
            return;
        }

        LockRequest head = queue.poll();
        currentHolder = head.requestId;
        head.result.complete(null);
    }

    private class LockRequest {
        private final CompletableFuture<Void> result = new CompletableFuture<>();
        private final UUID requestId;

        LockRequest(UUID requestId) {
            this.requestId = requestId;
        }
    }

}
