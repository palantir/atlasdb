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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Maps;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.LockDescriptor;

class LockWatch {
    private Map<LockDescriptor, LockIndexState> indexStates;
    private Map<LockDescriptor, AtomicLong> counters;

    LockWatch(Set<LockDescriptor> lockDescriptors) {
        this.indexStates = KeyedStream.of(lockDescriptors)
                .map(unused -> LockIndexState.DEFAULT)
                .collectTo(Maps::newConcurrentMap);
        this.counters = KeyedStream.of(lockDescriptors)
                .map(unused -> new AtomicLong())
                .collectTo(Maps::newConcurrentMap);
    }

    void registerLock(LockDescriptor descriptor) {
        indexStates.computeIfPresent(
                descriptor,
                (unused, oldState) -> oldState.withLockSequence(counters.get(descriptor).getAndIncrement()));
    }

    void registerUnlock(LockDescriptor descriptor) {
        indexStates.computeIfPresent(
                descriptor,
                (unused, oldState) -> oldState.withUnlockSequence(counters.get(descriptor).getAndIncrement()));
    }

    LockWatchState getState() {
        return ImmutableLockWatchState.builder()
                .putAllLockStates(indexStates)
                .build();
    }
}
