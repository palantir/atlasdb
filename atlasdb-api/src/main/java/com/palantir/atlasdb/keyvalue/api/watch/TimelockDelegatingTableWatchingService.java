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

package com.palantir.atlasdb.keyvalue.api.watch;

import java.util.Set;

import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.TableWatchingService;
import com.palantir.lock.watch.TimestampWithLockInfo;
import com.palantir.lock.watch.VersionedLockWatchState;

public class TimelockDelegatingTableWatchingService implements TableWatchingService {
    private final TimelockService timelock;

    public TimelockDelegatingTableWatchingService(TimelockService timelock) {
        this.timelock = timelock;
    }

    @Override
    public void registerWatches(Set<LockWatchReferences.LockWatchReference> lockWatchEntries) {
        // noop
    }

    @Override
    public VersionedLockWatchState getLockWatchState(long startTimestamp) {
        return VersionedLockWatchState.NONE;
    }

    @Override
    public TimestampWithLockInfo getCommitTimestampWithLockInfo(long startTimestamp) {
        long commitTs = timelock.getFreshTimestamp();
        return TimestampWithLockInfo.invalidate(commitTs);
    }
}
