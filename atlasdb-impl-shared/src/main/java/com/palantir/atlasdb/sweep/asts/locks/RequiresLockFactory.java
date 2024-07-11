/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.asts.locks;

import com.google.common.collect.MapMaker;
import com.palantir.atlasdb.sweep.asts.locks.RequiresLock.Lockable;
import com.palantir.lock.v2.TimelockService;
import java.util.concurrent.ConcurrentMap;

// This isn't really necessary, but it's a nice way to keep the optimisation around having a local lock check by sharing
// the underlying locked object, especially since this ended up being pretty simple.
public final class RequiresLockFactory<T extends Lockable> {
    // This _has_ to be weak references, unless we do some automatic pruning (but, why bother implementing that
    // ourselves when the GC already
    // exists?)
    // TODO: Check this actually works.
    private final ConcurrentMap<T, RequiresLock<T>> memoizedLocks =
            new MapMaker().weakKeys().weakValues().makeMap();
    private final TimelockService timelockService;

    private RequiresLockFactory(TimelockService timelockService) {
        this.timelockService = timelockService;
    }

    public static <T extends Lockable> RequiresLockFactory<T> create(TimelockService timelockService) {
        return new RequiresLockFactory<>(timelockService);
    }

    public RequiresLock<T> createLockable(T lockable) {
        return memoizedLocks.computeIfAbsent(lockable, value -> RequiresLock.create(value, timelockService));
    }
}
