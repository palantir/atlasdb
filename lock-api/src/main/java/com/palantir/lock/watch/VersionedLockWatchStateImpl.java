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

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.palantir.lock.LockDescriptor;

public class VersionedLockWatchStateImpl implements VersionedLockWatchState {
    private final OptionalLong version;
    private final RangeMap<LockDescriptor, LockWatchInfo> watchedRanges;
    private final Map<LockDescriptor, LockWatchInfo> locks;

    public VersionedLockWatchStateImpl(OptionalLong version,
            RangeMap<LockDescriptor, LockWatchInfo> watchedRanges,
            Map<LockDescriptor, LockWatchInfo> locks) {
        this.version = version;
        this.watchedRanges = watchedRanges;
        this.locks = locks;
    }

    @Override
    public OptionalLong version() {
        return version;
    }

    @Override
    public LockWatchInfo lockWatchState(LockDescriptor lockDescriptor) {
        if (locks.containsKey(lockDescriptor)) {
            return locks.get(lockDescriptor);
        }
        Map.Entry<Range<LockDescriptor>, LockWatchInfo> watchedRange = watchedRanges.getEntry(lockDescriptor);
        if (watchedRange != null) {
            return watchedRange.getValue();
        }
        return ImmutableLockWatchInfo.of(OptionalLong.empty(), LockWatchInfo.State.LOCKED);
    }
}
