/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock.lock;

import com.palantir.atlasdb.timelock.api.TimestampLeaseName;
import com.palantir.atlasdb.timelock.timestampleases.TimestampLeaseMetrics;
import com.palantir.lock.LockDescriptor;
import java.util.Optional;
import java.util.Set;

final class LockManager {
    private final ExclusiveLockCollection exclusiveLocks;
    private final NamedMinTimestampLockCollection namedMinTimestampLockCollection;

    private LockManager(
            ExclusiveLockCollection exclusiveLocks, NamedMinTimestampLockCollection namedMinTimestampLockCollection) {
        this.exclusiveLocks = exclusiveLocks;
        this.namedMinTimestampLockCollection = namedMinTimestampLockCollection;
    }

    static LockManager create(TimestampLeaseMetrics metrics) {
        return new LockManager(new ExclusiveLockCollection(), NamedMinTimestampLockCollection.create(metrics));
    }

    OrderedLocks getAllExclusiveLocks(Set<LockDescriptor> descriptors) {
        return exclusiveLocks.getAll(descriptors);
    }

    Optional<Long> getNamedMinTimestamp(TimestampLeaseName timestampName) {
        return namedMinTimestampLockCollection.getNamedMinTimestamp(timestampName);
    }

    AsyncLock getNamedTimestampLock(TimestampLeaseName timestampName, long timestamp) {
        return namedMinTimestampLockCollection.getNamedTimestampLock(timestampName, timestamp);
    }

    Optional<Long> getImmutableTimestamp() {
        return namedMinTimestampLockCollection.getImmutableTimestamp();
    }

    AsyncLock getImmutableTimestampLock(long timestamp) {
        return namedMinTimestampLockCollection.getImmutableTimestampLock(timestamp);
    }
}
