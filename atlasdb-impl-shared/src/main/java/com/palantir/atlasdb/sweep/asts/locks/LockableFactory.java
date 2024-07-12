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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.TimelockService;
import com.palantir.refreshable.Refreshable;
import java.time.Duration;
import java.util.function.Function;

// This isn't necessary for correctness, but it's a nice way to keep the optimisation around having a local
// lock check by sharing the underlying locked object, especially since this ended up being pretty simple (as long
// as it's correct!)

// rough notes:
// This implementation ended up simplifying a bunch of things around local lock management.
// It is possible for us to instead _not_ use the value inside the locked object, but that makes it _much_ harder
// to keep track of the objects that are around, as you'll only construct the locked object as you need it (and so
// you have no reference to it!) and so can't rely on a weakref cache.
// By keeping the object in the locked value, then you have to keep the locked value around, so you keep the reference
// alive.
// Or, you cache the underlying immutable objects and not recreate them whenever you reload, but that pushes the
// problem elsewhere.
public final class LockableFactory<T> {
    // This _has_ to be weak references, unless we do some manual pruning (but, why bother implementing that
    // ourselves when the GC already exists?)
    // Cannot be weak keys, since weak keys means identity equality not equals.
    private final LoadingCache<T, Lockable<T>> memoizedLocks;

    private LockableFactory(
            TimelockService timelockService,
            Refreshable<Duration> lockTimeout,
            Function<T, LockDescriptor> lockDescriptorFunction) {
        this.memoizedLocks = Caffeine.newBuilder()
                .weakValues()
                .build(key -> Lockable.create(key, lockDescriptorFunction.apply(key), timelockService, lockTimeout));
    }

    public static <T> LockableFactory<T> create(
            TimelockService timelockService,
            Refreshable<Duration> lockTimeout,
            Function<T, LockDescriptor> lockDescriptorFunction) {
        return new LockableFactory<>(timelockService, lockTimeout, lockDescriptorFunction);
    }

    public Lockable<T> createLockable(T lockable) {
        return memoizedLocks.get(lockable);
    }
}
