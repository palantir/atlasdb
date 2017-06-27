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

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.lock.v2.LockTokenV2;

public class HeldLocksCollection {

    @VisibleForTesting
    final ConcurrentMap<UUID, CompletableFuture<HeldLocks>> heldLocksById = Maps.newConcurrentMap();

    public CompletableFuture<LockTokenV2> getExistingOrAcquire(
            UUID requestId,
            Supplier<CompletableFuture<HeldLocks>> lockAcquirer) {
        CompletableFuture<HeldLocks> locksFuture = heldLocksById.computeIfAbsent(
                requestId, ignored -> lockAcquirer.get());
        return locksFuture.thenApply(HeldLocks::getToken);
    }

    public Set<LockTokenV2> unlock(Set<LockTokenV2> tokens) {
        Set<LockTokenV2> unlocked = filter(tokens, HeldLocks::unlock);
        for (LockTokenV2 token : unlocked) {
            heldLocksById.remove(token.getRequestId());
        }
        return unlocked;
    }

    public Set<LockTokenV2> refresh(Set<LockTokenV2> tokens) {
        return filter(tokens, HeldLocks::refresh);
    }

    public void removeExpired() {
        Iterator<CompletableFuture<HeldLocks>> iterator = heldLocksById.values().iterator();
        while (iterator.hasNext()) {
            CompletableFuture<HeldLocks> locksFuture = iterator.next();
            if (isFailed(locksFuture)) {
                iterator.remove();
            } else {
                Optional<HeldLocks> heldLocks = getIfCompleted(locksFuture);
                if (heldLocks.isPresent() && heldLocks.get().unlockIfExpired()) {
                    iterator.remove();
                }
            }
        }
    }

    private Optional<HeldLocks> getCompleted(UUID requestId) {
        CompletableFuture<HeldLocks> locksFuture = heldLocksById.get(requestId);
        if (locksFuture == null) {
            return Optional.empty();
        }

        return getIfCompleted(locksFuture);
    }

    private Set<LockTokenV2> filter(Set<LockTokenV2> tokens, Predicate<HeldLocks> predicate) {
        Set<LockTokenV2> filtered = Sets.newHashSetWithExpectedSize(tokens.size());

        for (LockTokenV2 token : tokens) {
            Optional<HeldLocks> heldLocks = getCompleted(token.getRequestId());
            if (heldLocks.isPresent() && predicate.test(heldLocks.get())) {
                filtered.add(token);
            }
        }

        return filtered;
    }

    private boolean isFailed(CompletableFuture<?> future) {
        return future.isCancelled() || future.isCompletedExceptionally();
    }

    private Optional<HeldLocks> getIfCompleted(CompletableFuture<HeldLocks> future) {
        if (future.isDone() && !isFailed(future)) {
            return Optional.of(future.join());
        }
        return Optional.empty();
    }

}
