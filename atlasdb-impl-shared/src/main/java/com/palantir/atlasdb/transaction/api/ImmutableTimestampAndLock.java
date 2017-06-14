/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.api;


import com.palantir.lock.LockRefreshToken;

public class ImmutableTimestampAndLock {

    private final long immutableTimestamp;
    private final LockRefreshToken lock;

    public ImmutableTimestampAndLock(long immutableTimestamp, LockRefreshToken lock) {
        this.immutableTimestamp = immutableTimestamp;
        this.lock = lock;
    }

    public long getImmutableTimestamp() {
        return immutableTimestamp;
    }

    public LockRefreshToken getLock() {
        return lock;
    }
}
