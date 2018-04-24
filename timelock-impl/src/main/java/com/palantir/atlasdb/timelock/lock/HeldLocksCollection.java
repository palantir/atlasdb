/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.lock.v2.LockToken;

public class HeldLocksCollection {

    @VisibleForTesting
    final ConcurrentMap<UUID, AsyncResult<HeldLocks>> heldLocksById = Maps.newConcurrentMap();

    public AsyncResult<LockToken> getExistingOrAcquire(
            UUID requestId,
            Supplier<AsyncResult<HeldLocks>> lockAcquirer) {
        AsyncResult<HeldLocks> locksFuture = heldLocksById.computeIfAbsent(
                requestId, ignored -> lockAcquirer.get());
        return locksFuture.map(HeldLocks::getToken);
    }

    public Set<LockToken> unlock(Set<LockToken> tokens) {
        Set<LockToken> unlocked = filter(tokens, HeldLocks::unlock);
        for (LockToken token : unlocked) {
            heldLocksById.remove(token.getRequestId());
        }
        return unlocked;
    }

    public Set<LockToken> refresh(Set<LockToken> tokens) {
        return filter(tokens, HeldLocks::refresh);
    }

    public void removeExpired() {
        Iterator<AsyncResult<HeldLocks>> iterator = heldLocksById.values().iterator();
        while (iterator.hasNext()) {
            AsyncResult<HeldLocks> lockResult = iterator.next();
            if (shouldRemove(lockResult)) {
                iterator.remove();
            }
        }
    }

    public void failAllOutstandingRequestsWithNotCurrentLeaderException() {
        NotCurrentLeaderException ex = new NotCurrentLeaderException("This lock service has been closed");
        heldLocksById.values().forEach(result -> result.failIfNotCompleted(ex));
    }

    private boolean shouldRemove(AsyncResult<HeldLocks> lockResult) {
        return lockResult.isFailed()
                || lockResult.isTimedOut()
                || lockResult.test(HeldLocks::unlockIfExpired);
    }

    private Set<LockToken> filter(Set<LockToken> tokens, Predicate<HeldLocks> predicate) {
        Set<LockToken> filtered = Sets.newHashSetWithExpectedSize(tokens.size());

        for (LockToken token : tokens) {
            AsyncResult<HeldLocks> lockResult = heldLocksById.get(token.getRequestId());
            if (lockResult != null && lockResult.test(predicate)) {
                filtered.add(token);
            }
        }

        return filtered;
    }

}
