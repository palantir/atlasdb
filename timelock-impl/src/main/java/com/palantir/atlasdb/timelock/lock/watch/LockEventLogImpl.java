/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.lock.watch;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;

import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchOpenLocksEvent;
import com.palantir.lock.watch.LockWatchRequest;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.UnlockEvent;

public class LockEventLogImpl implements LockEventLog {
    private final UUID leaderId = UUID.randomUUID();
    private final ArrayLockEventSlidingWindow slidingWindow = new ArrayLockEventSlidingWindow(1000);

    @Override
    public LockWatchStateUpdate getLogDiff(OptionalLong fromVersion) {
        if (!fromVersion.isPresent()) {
            return LockWatchStateUpdate.failure(leaderId, slidingWindow.getVersion());
        }
        Optional<List<LockWatchEvent>> maybeEvents = slidingWindow.getFromVersion(fromVersion.getAsLong());
        if (!maybeEvents.isPresent()) {
            return LockWatchStateUpdate.failure(leaderId, slidingWindow.getVersion());
        }
        List<LockWatchEvent> events = maybeEvents.get();
        return LockWatchStateUpdate
                .of(leaderId, true, OptionalLong.of(fromVersion.getAsLong() + events.size()), events);
    }

    @Override
    public void logLock(Set<LockDescriptor> locksTakenOut, LockToken lockToken) {
        slidingWindow.add(LockEvent.builder(locksTakenOut, lockToken));
    }

    @Override
    public void logUnlock(Set<LockDescriptor> locksUnlocked) {
        slidingWindow.add(UnlockEvent.builder(locksUnlocked));
    }

    @Override
    public void logOpenLocks(Set<LockDescriptor> openLocks, UUID requestId) {
        slidingWindow.add(LockWatchOpenLocksEvent.builder(openLocks, requestId));
    }

    @Override
    public void logLockWatchCreated(LockWatchRequest locksToWatch, UUID requestId) {
        slidingWindow.add(LockWatchCreatedEvent.builder(locksToWatch, requestId));
    }
}
