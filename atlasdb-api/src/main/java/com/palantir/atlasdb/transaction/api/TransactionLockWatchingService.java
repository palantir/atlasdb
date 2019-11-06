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

import java.util.Set;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.lock.watch.LockWatchState;

public interface TransactionLockWatchingService {
    LockDescriptorMapping registerRowWatches(TableReference tableRef, Set<byte[]> rowNames);
    void deregisterWatches(TableReference tableRef, Set<byte[]> rowNames);
    LockWatchState getLockWatchState();

    class AlwaysThrowingTransactionLockWatchingService implements TransactionLockWatchingService {
        @Override
        public LockDescriptorMapping registerRowWatches(TableReference tableRef, Set<byte[]> rowNames) {
            throw new UnsupportedOperationException("This LockWatchingService does not support this operation.");
        }
        @Override
        public void deregisterWatches(TableReference tableRef, Set<byte[]> rowNames) {
            throw new UnsupportedOperationException("This LockWatchingService does not support this operation.");
        }
        @Override
        public LockWatchState getLockWatchState() {
            throw new UnsupportedOperationException("This LockWatchingService does not support this operation.");
        }
    }
}
