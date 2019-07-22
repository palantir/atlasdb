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

package com.palantir.atlasdb.timelock.watch;

import java.util.concurrent.atomic.AtomicLong;

class LockWatch {
    public static final int NO_VALUE = 0;

    private final AtomicLong counter;

    private volatile long lastLock;
    private volatile long lastUnlock;

    LockWatch() {
        this.counter = new AtomicLong();
        this.lastLock = NO_VALUE;
        this.lastUnlock = NO_VALUE;
    }

    void registerLock() {
        this.lastLock = counter.incrementAndGet();
    }

    void registerUnlock() {
        this.lastUnlock = counter.incrementAndGet();
    }

    WatchIndexState getState() {
        // Possible that this reads lock THEN unlock is updated THEN this reads unlock. That's okay.
        return ImmutableWatchIndexState.builder()
                .lastLockSequence(lastLock)
                .lastUnlockSequence(lastUnlock)
                .build();
    }
}
