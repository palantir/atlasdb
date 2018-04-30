/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.timelock.lock;

import java.util.UUID;

import javax.ws.rs.NotSupportedException;

import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;

public class ImmutableTimestampLock implements AsyncLock {

    private final long timestamp;
    private final ImmutableTimestampTracker tracker;

    public ImmutableTimestampLock(long timestamp, ImmutableTimestampTracker tracker) {
        this.timestamp = timestamp;
        this.tracker = tracker;
    }

    @Override
    public AsyncResult<Void> lock(UUID requestId) {
        tracker.lock(timestamp, requestId);
        return AsyncResult.completedResult();
    }

    @Override
    public AsyncResult<Void> waitUntilAvailable(UUID requestId) {
        throw new NotSupportedException();
    }

    @Override
    public void unlock(UUID requestId) {
        tracker.unlock(timestamp, requestId);
    }

    @Override
    public void timeout(UUID requestId) {
        throw new NotSupportedException();
    }

    @Override
    public LockDescriptor getDescriptor() {
        return StringLockDescriptor.of("ImmutableTimestamp:" + timestamp);
    }
}
