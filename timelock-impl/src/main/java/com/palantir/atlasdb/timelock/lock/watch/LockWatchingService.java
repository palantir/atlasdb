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

package com.palantir.atlasdb.timelock.lock.watch;

import java.util.Set;
import java.util.UUID;

import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockWatchState;
import com.palantir.lock.watch.WatchIdToLockDesciptor;

public interface LockWatchingService {
    WatchIdToLockDesciptor startWatching(UUID serviceId, Set<LockDescriptor> locksToWatch);
    void stopWatching(UUID serviceId, Set<LockDescriptor> locksToUnwatch);
    LockWatchState getWatchState(UUID serviceId);

    void registerLock(Set<LockDescriptor> locksTakenOut, LockToken lockToken);
    void registerUnlock(LockToken lockToken);
}
