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

import java.util.Optional;
import java.util.UUID;

public class ImmutableTimestampTracker {
    private final MinimumTimestampLessorImpl lessor = new MinimumTimestampLessorImpl();

    public void lock(long timestamp, UUID requestId) {
        lessor.hold(timestamp, requestId);
    }

    public void unlock(long timestamp, UUID requestId) {
        lessor.release(timestamp, requestId);
    }

    public Optional<Long> getImmutableTimestamp() {
        return lessor.getMinimumLeased();
    }

    // TODO(nziebart): should these locks should be created by LockCollection for consistency?
    public AsyncLock getLockFor(long timestamp) {
        return new ImmutableTimestampLock(timestamp, this);
    }
}
