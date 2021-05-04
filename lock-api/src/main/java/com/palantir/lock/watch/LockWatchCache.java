/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.watch;

import java.util.Collection;
import java.util.Set;

public interface LockWatchCache {
    void processStartTransactionsUpdate(Set<Long> startTimestamps, LockWatchStateUpdate update);

    void processCommitTimestampsUpdate(Collection<TransactionUpdate> transactionUpdates, LockWatchStateUpdate update);

    void updateCacheAndRemoveTransactionState(long startTimestamp);

    void removeTransactionStateFromCache(long startTimestamp);

    LockWatchEventCache getEventCache();

    LockWatchValueCache getValueCache();
}
