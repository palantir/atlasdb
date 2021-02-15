/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.collect.RangeSet;
import com.palantir.lock.LockDescriptor;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

public class VersionedLockWatchStateImpl implements VersionedLockWatchState {
    private final OptionalLong version;
    private final RangeSet<LockDescriptor> watchedRanges;
    private final Map<LockDescriptor, LockWatchInfo> locks;
    private final UUID leaderId;
    private final LockWatchStateUpdate lastUpdate;

    public VersionedLockWatchStateImpl(
            OptionalLong version,
            RangeSet<LockDescriptor> watchedRanges,
            Map<LockDescriptor, LockWatchInfo> locks,
            UUID leaderId,
            LockWatchStateUpdate lastUpdate) {
        this.version = version;
        this.watchedRanges = watchedRanges;
        this.locks = locks;
        this.leaderId = leaderId;
        this.lastUpdate = lastUpdate;
    }

    @Override
    public OptionalLong version() {
        return version;
    }

    @Override
    public UUID leaderId() {
        return leaderId;
    }

    @Override
    public LockWatchInfo lockWatchState(LockDescriptor lockDescriptor) {
        Optional<LockWatchInfo> explicitInfo = Optional.ofNullable(locks.get(lockDescriptor));
        return explicitInfo.orElseGet(() -> deriveWatchInfo(lockDescriptor));
    }

    private LockWatchInfo deriveWatchInfo(LockDescriptor lockDescriptor) {
        if (watchedRanges.contains(lockDescriptor)) {
            return LockWatchInfo.of(LockWatchInfo.State.UNLOCKED, OptionalLong.empty());
        }
        return LockWatchInfos.UNKNOWN;
    }

    @Override
    public LockWatchStateUpdate lastUpdate() {
        return lastUpdate;
    }
}
