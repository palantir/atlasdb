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

import com.palantir.common.annotation.Idempotent;
import com.palantir.lock.watch.LockWatchInfo;
import com.palantir.lock.watch.WatchId;

public interface TableWatchingService {
    /**
     * Registers watches
     */
    @Idempotent
    void registerWatches(TableElements lockWatchEntries);

    /**
     * Deregisters watches
     */
    void deregisterWatches(TableElements lockWatchEntries);

    /**
     * Returns the current state of all registered watches.
     * @return a mapping of {@link WatchId}s to their corresponding {@link LockWatchInfo}
     */
    TableWatchState getLockWatchState(Optional<Long> lastKnownState);
}
