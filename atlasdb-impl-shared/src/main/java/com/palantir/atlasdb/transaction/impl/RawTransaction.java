/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.lock.v2.LockToken;

public class RawTransaction extends ForwardingTransaction {
    private final SnapshotTransaction delegate;
    private final LockToken lock;

    public RawTransaction(SnapshotTransaction delegate, LockToken lock) {
        this.delegate = delegate;
        this.lock = lock;
    }

    @Override
    public SnapshotTransaction delegate() {
        return delegate;
    }

    LockToken getImmutableTsLock() {
        return lock;
    }
}
