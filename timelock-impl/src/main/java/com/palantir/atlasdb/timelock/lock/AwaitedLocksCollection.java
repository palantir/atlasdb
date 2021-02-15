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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

// TODO(nziebart): can we combine this logic with HeldLocksCollection?
// Or, should awaitLocks just be implemented by a lock + unlock?
public class AwaitedLocksCollection {

    @VisibleForTesting
    final ConcurrentMap<UUID, AsyncResult<Void>> requestsById = new ConcurrentHashMap<>();

    public AsyncResult<Void> getExistingOrAwait(UUID requestId, Supplier<AsyncResult<Void>> lockAwaiter) {
        AsyncResult<Void> result = requestsById.computeIfAbsent(requestId, ignored -> lockAwaiter.get());

        registerCompletionHandler(requestId, result);
        return result;
    }

    private void registerCompletionHandler(UUID requestId, AsyncResult<Void> result) {
        // Modifying the map synchronously in this callback can deadlock.
        // The thread that completes this result will be inside a synchronized method on AsyncLock; if a supplier
        // passed to #computeIfAbsent simultaneously tries to call a method on the same AsyncLock, we will deadlock.
        result.onCompleteAsync(() -> requestsById.remove(requestId));
    }
}
