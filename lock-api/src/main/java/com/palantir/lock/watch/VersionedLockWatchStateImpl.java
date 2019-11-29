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

import com.google.common.collect.RangeSet;
import com.palantir.lock.LockDescriptor;

public class VersionedLockWatchStateImpl implements VersionedLockWatchState {
    private final OptionalLong version;
    private final RangeSet<LockDescriptor> watchedRanges;
    private final Map<LockDescriptor, LockWatchState> locks;

    public VersionedLockWatchStateImpl(OptionalLong version,
            RangeSet<LockDescriptor> watchedRanges,
            Map<LockDescriptor, LockWatchState> locks) {
        this.version = version;
        this.watchedRanges = watchedRanges;
        this.locks = locks;
    }

    @Override
    public OptionalLong version() {
        return version;
    }

    @Override
    public LockWatchState lockWatchState(LockDescriptor lockDescriptor) {
        if (locks.containsKey(lockDescriptor)) {
            return locks.get(lockDescriptor);
        }
        if (watchedRanges.contains(lockDescriptor)) {
            return LockWatchState.UNLOCKED;
        }
        return LockWatchState.NOT_WATCHED;
    }
}
