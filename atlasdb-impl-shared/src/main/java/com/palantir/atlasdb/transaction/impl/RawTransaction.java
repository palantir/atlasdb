/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.transaction.impl;

import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.lock.LockRefreshToken;

public class RawTransaction extends ForwardingTransaction {
    private final PollingSnapshotTransaction delegate;
    private final LockRefreshToken lock;

    public RawTransaction(PollingSnapshotTransaction delegate, LockRefreshToken lock) {
        this.delegate = delegate;
        this.lock = lock;
    }

    public RawTransaction(SnapshotTransaction snapshotTransaction, LockRefreshToken lock) {
        this.delegate = new PollingSnapshotTransaction(snapshotTransaction, false);
        this.lock = lock;
    }

    @Override
    public PollingSnapshotTransaction delegate() {
        return delegate;
    }

    LockRefreshToken getImmutableTsLock() {
        return lock;
    }
}
