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

import com.google.common.collect.RangeSet;
import com.palantir.lock.LockDescriptor;

public class LockWatchStateEventVisitor implements LockWatchEvent.Visitor {
    private final RangeSet<LockDescriptor> watches;
    private final Map<LockDescriptor, LockWatchState> lockWatchState;

    public LockWatchStateEventVisitor(
            RangeSet<LockDescriptor> watches,
            Map<LockDescriptor, LockWatchState> lockWatchState) {
        this.watches = watches;
        this.lockWatchState = lockWatchState;
    }

    @Override
    public void visit(LockEvent lockEvent) {
        for (LockDescriptor descriptor: lockEvent.lockDescriptors()) {
            lockWatchState.put(descriptor, LockWatchState.LOCKED);
        }
    }

    @Override
    public void visit(UnlockEvent unlockEvent) {
        for (LockDescriptor descriptor: unlockEvent.lockDescriptors()) {
            lockWatchState.put(descriptor, LockWatchState.UNLOCKED);
        }
    }

    @Override
    public void visit(LockWatchOpenLocksEvent openLocksEvent) {
        for (LockDescriptor descriptor: openLocksEvent.lockDescriptors()) {
            lockWatchState.put(descriptor, LockWatchState.LOCKED);
        }    }

    @Override
    public void visit(LockWatchCreatedEvent lockWatchCreatedEvent) {
        watches.addAll(lockWatchCreatedEvent.request().ranges());
    }
}
