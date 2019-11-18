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

import java.util.Optional;

import com.palantir.atlasdb.transaction.api.KvsLockWatchingService;
import com.palantir.lock.watch.LockWatchRequest;

public class TableWatchingServiceAdapter implements TableWatchingService {
    private final KvsLockWatchingService kvsLockWatchingService;

    public TableWatchingServiceAdapter(KvsLockWatchingService kvsLockWatchingService) {
        this.kvsLockWatchingService = kvsLockWatchingService;
    }

    @Override
    public void registerWatches(TableElements lockWatchEntries) {
        kvsLockWatchingService.registerWatches(LockWatchRequest.of(lockWatchEntries.asLockDescriptors()));
    }

    @Override
    public void deregisterWatches(TableElements lockWatchEntries) {
        kvsLockWatchingService.deregisterWatches(LockWatchRequest.of(lockWatchEntries.asLockDescriptors()));
    }

    @Override
    public TableWatchState getLockWatchState(Optional<Long> lastKnownState) {
        return null;
    }
}
