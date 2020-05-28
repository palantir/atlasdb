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

package com.palantir.atlasdb.transaction.impl.watch;

import java.util.Set;

import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManager;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.LockWatchReferences;

public final class LockWatchManagerImpl implements LockWatchManager {

    private final LockWatchEventCache cache;

    public LockWatchManagerImpl(LockWatchEventCache cache) {
        this.cache = cache;
    }

    @Override
    public void registerWatches(Set<LockWatchReferences.LockWatchReference> lockWatchReferences) {
        cache.registerLockWatches(lockWatchReferences);
    }
}
