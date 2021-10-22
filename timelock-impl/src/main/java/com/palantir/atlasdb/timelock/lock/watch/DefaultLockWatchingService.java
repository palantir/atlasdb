/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.atlasdb.timelock.lock.HeldLocksCollection;
import com.palantir.atlasdb.timelock.lock.LeaderClock;

public class DefaultLockWatchingService {
    private final LeaderClock clock;
    private final HeldLocksCollection heldLocks;
    private final LockWatchingService lockWatchingService;

    public DefaultLockWatchingService() {
        this.clock = LeaderClock.create();
        this.heldLocks = HeldLocksCollection.create(clock);
        this.lockWatchingService = new LockWatchingServiceImpl(heldLocks, clock.id());
    }

    public LockWatchingService get() {
        return lockWatchingService;
    }

    public LeaderClock clock() {
        return clock;
    }

    public HeldLocksCollection heldLocks() {
        return heldLocks;
    }
}
