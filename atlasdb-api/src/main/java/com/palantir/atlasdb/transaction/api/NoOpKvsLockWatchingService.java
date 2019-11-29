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

package com.palantir.atlasdb.transaction.api;

import java.util.OptionalLong;
import java.util.Set;

import com.palantir.lock.LockDescriptor;
import com.palantir.lock.watch.LockWatchRequest;
import com.palantir.lock.watch.VersionedLockWatchState;

public final class NoOpKvsLockWatchingService implements KvsLockWatchingService {
    public static final KvsLockWatchingService INSTANCE = new NoOpKvsLockWatchingService();

    private NoOpKvsLockWatchingService() {
        // nope
    }

    @Override
    public void registerWatches(LockWatchRequest lockWatchEntries) {
        // noop
    }

    @Override
    public VersionedLockWatchState getLockWatchState() {
        throw new UnsupportedOperationException("not implemented yet");
    }

    @Override
    public Set<LockDescriptor> lockedSinceVersion(long version) {
        throw new UnsupportedOperationException("not implemented yet");
    }
}
