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
import java.util.OptionalLong;
import java.util.stream.Collectors;

import com.google.common.collect.RangeSet;
import com.palantir.lock.LockDescriptor;

public class LockWatchStateUpdater implements LockWatchEvent.Visitor<Void> {
    private final RangeSet<LockDescriptor> watches;
    private final Map<LockDescriptor, LockWatchInfo> lockWatchState;

    public LockWatchStateEventVisitor(
            RangeSet<LockDescriptor> watches,
            Map<LockDescriptor, LockWatchInfo> lockWatchState) {
        this.watches = watches;
        this.lockWatchState = lockWatchState;
    }

    @Override
    public Void visit(LockEvent lockEvent) {
        for (LockDescriptor descriptor : lockEvent.lockDescriptors()) {
            lockWatchState.put(descriptor, LockWatchInfo.of(LockWatchInfo.State.LOCKED, lockEvent.sequence()));
        }
        return null;
    }

    @Override
    public Void visit(UnlockEvent unlockEvent) {
        for (LockDescriptor descriptor : unlockEvent.lockDescriptors()) {
            OptionalLong lastLocked = lockWatchState.get(descriptor).lastLocked();
            lockWatchState.put(descriptor, LockWatchInfo.of(LockWatchInfo.State.UNLOCKED, lastLocked));
        }
        return null;
    }

    @Override
    public Void visit(LockWatchOpenLocksEvent openLocksEvent) {
        for (LockDescriptor descriptor : openLocksEvent.lockDescriptors()) {
            lockWatchState.put(descriptor, LockWatchInfo.of(LockWatchInfo.State.LOCKED, openLocksEvent.sequence()));
        }
        return null;
    }

    @Override
    public Void visit(LockWatchCreatedEvent lockWatchCreatedEvent) {
        watches.addAll(lockWatchCreatedEvent.request().references().stream()
                .map(ref -> ref.accept(LockWatchReferences.TO_RANGES_VISITOR))
                .collect(Collectors.toList()));
        return null;
    }
}