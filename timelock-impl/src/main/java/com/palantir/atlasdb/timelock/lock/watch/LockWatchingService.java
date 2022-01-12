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

import com.palantir.lock.LockDescriptor;
import com.palantir.lock.client.LockWatchStarter;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.LockWatchVersion;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

public interface LockWatchingService extends LockWatchStarter {
    LockWatchStateUpdate getWatchStateUpdate(Optional<LockWatchVersion> lastKnownVersion);

    <T> ValueAndLockWatchStateUpdate<T> runTask(Optional<LockWatchVersion> lastKnownVersion, Supplier<T> task);

    void registerLock(Set<LockDescriptor> locksTakenOut, LockToken token);

    void registerUnlock(Set<LockDescriptor> locksUnlocked);
}
