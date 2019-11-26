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

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.palantir.lock.LockDescriptor;

public class LockWatchStateEventVisitor implements LockWatchEventVisitor {
    private final RangeMap<LockDescriptor, LockWatchInfo> lockWatchState;

    public LockWatchStateEventVisitor(
            RangeMap<LockDescriptor, LockWatchInfo> lockWatchState) {
        this.lockWatchState = lockWatchState;
    }

    @Override
    public void visit(LockEvent lockEvent) {
        for (LockDescriptor descriptor: lockEvent.lockDescriptors()) {
            Map.Entry<Range<LockDescriptor>, LockWatchInfo> entry = lockWatchState.getEntry(descriptor);
            if (entry == null) {
                lockWatchState.put(Range.singleton(descriptor),
                        ImmutableLockWatchInfo.of(OptionalLong.of(lockEvent.sequence()), LockWatchInfo.State.LOCKED));
            } else {
                LockWatchInfo currentInfo = entry.getValue();
                if (currentInfo.lastChange().isPresent() && currentInfo.lastChange().getAsLong() < lockEvent.sequence()) {
                    lockWatchState.put(Range.singleton(descriptor),
                            ImmutableLockWatchInfo.of(OptionalLong.of(lockEvent.sequence()), LockWatchInfo.State.LOCKED));
                }
            }
        }
    }

    @Override
    public void visit(UnlockEvent unlockEvent) {

    }

    @Override
    public void visit(LockWatchOpenLocksEvent openLocksEvent) {

    }

    @Override
    public void visit(LockWatchCreatedEvent lockWatchCreatedEvent) {
        for (Range<LockDescriptor> range: lockWatchCreatedEvent.request().ranges()) {
            RangeMap<LockDescriptor, LockWatchInfo> update = TreeRangeMap.create();
            update.put(range, ImmutableLockWatchInfo.of(OptionalLong.empty(), LockWatchInfo.State.LOCKED));
            RangeMap<LockDescriptor, LockWatchInfo> existingRangeMappings = lockWatchState.subRangeMap(range);
            for (Map.Entry<Range<LockDescriptor>, LockWatchInfo> entry: existingRangeMappings.asMapOfRanges().entrySet()) {

            }
        }
    }
}
