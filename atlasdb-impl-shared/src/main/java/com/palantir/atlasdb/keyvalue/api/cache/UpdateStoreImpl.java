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

package com.palantir.atlasdb.keyvalue.api.cache;

import com.palantir.atlasdb.keyvalue.api.AtlasLockDescriptorUtils;
import com.palantir.atlasdb.keyvalue.api.watch.StartTimestamp;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.CommitUpdate.Visitor;
import com.palantir.lock.watch.LockWatchEventCache;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
final class UpdateStoreImpl implements UpdateStore {
    private final Map<StartTimestamp, CellUpdate> updateMap;

    UpdateStoreImpl() {
        updateMap = new ConcurrentHashMap<>();
    }

    @Override
    public CellUpdate getUpdate(LockWatchEventCache eventCache, StartTimestamp startTimestamp) {
        Optional<CellUpdate> cellUpdate = Optional.ofNullable(updateMap.get(startTimestamp));

        // This approach guarantees that if we read it, we return it, otherwise we update a value (we cannot put then
        // get from the map in case we were to reset in between).
        if (cellUpdate.isPresent()) {
            return cellUpdate.get();
        } else {
            CellUpdate update = toCellUpdate(eventCache.getCommitUpdate(startTimestamp.value()));
            updateMap.put(startTimestamp, update);
            return update;
        }
    }

    @Override
    public void removeUpdate(StartTimestamp startTimestamp) {
        updateMap.remove(startTimestamp);
    }

    @Override
    public void reset() {
        updateMap.clear();
    }

    private static CellUpdate toCellUpdate(CommitUpdate commitUpdate) {
        return commitUpdate.accept(new Visitor<CellUpdate>() {
            @Override
            public CellUpdate invalidateAll() {
                return CellUpdate.invalidateAll();
            }

            @Override
            public CellUpdate invalidateSome(Set<LockDescriptor> invalidatedLocks) {
                return CellUpdate.invalidateSome(invalidatedLocks.stream()
                        .map(AtlasLockDescriptorUtils::candidateCells)
                        .flatMap(List::stream)
                        .collect(Collectors.toSet()));
            }
        });
    }
}
