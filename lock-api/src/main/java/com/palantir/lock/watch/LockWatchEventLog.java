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

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.palantir.lock.LockDescriptor;

public abstract class LockWatchEventLog {
    private final AtomicReference<RangeMap<LockDescriptor, LockWatchInfo>> log = new AtomicReference<>(
            TreeRangeMap.create());
    private volatile Optional<Long> lastKnownVersion = Optional.empty();

    synchronized VersionedLockWatchState currentState() {
        return ImmutableVersionedLockWatchState.of(lastKnownVersion, log.get());
    }

    abstract void updateState(LockWatchStateUpdate update);
}
