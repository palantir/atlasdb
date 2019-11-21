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

import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;

import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchRequest;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.UnlockEvent;

public class LockEventLogImpl implements LockEventLog {
    private final UUID leaderId = UUID.randomUUID();
    private final ArrayLockEventSlidingWindow slidingWindow = new ArrayLockEventSlidingWindow(1000);

    @Override
    public LockWatchStateUpdate getLogDiff(OptionalLong fromVersion) {
        return LockWatchStateUpdate.of(leaderId, slidingWindow.getFromVersion(fromVersion));
    }

    @Override
    public void logLock(LockToken lockToken, Set<LockDescriptor> locksTakenOut) {
        slidingWindow.add(LockEvent.fromSeq(lockToken, locksTakenOut));
    }

    @Override
    public void logUnlock(LockToken lockToken, Set<LockDescriptor> locksUnlocked) {
        slidingWindow.add(UnlockEvent.fromSeq(lockToken, locksUnlocked));
    }

    @Override
    public void logOpenLocks(LockToken lockToken, Set<LockDescriptor> openLocks) {
        slidingWindow.add(LockEvent.fromSeq(lockToken, openLocks));
    }

    @Override
    public void logLockWatchCreated(LockWatchRequest locksToWatch) {
        slidingWindow.add(LockWatchCreatedEvent.fromSeq(locksToWatch));
    }
}
