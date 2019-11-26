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

package com.palantir.lock.watch;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.palantir.lock.LockDescriptor;

public abstract class LockWatchEventLog {
    private final AtomicReference<RangeMap<LockDescriptor, LockWatchInfo>> watches = new AtomicReference<>(
            TreeRangeMap.create());
    private final AtomicReference<Map<LockDescriptor, LockWatchInfo>> singleLocks = new AtomicReference<>(ImmutableMap.of());
    private volatile OptionalLong lastKnownVersion = OptionalLong.empty();
    private UUID leaderId = UUID.randomUUID();

    synchronized VersionedLockWatchState currentState() {
        return new VersionedLockWatchStateImpl(lastKnownVersion, watches.get(), singleLocks.get());
    }

    void updateState(LockWatchStateUpdate update) {
        if (leaderId != update.leaderId() || !lastKnownVersion.isPresent() || update.events().isEmpty()
                || update.events().get(0).sequence() != lastKnownVersion.getAsLong()) {
            resetAll(update);
        }

    }

    protected abstract void resetAll(LockWatchStateUpdate update);
}
