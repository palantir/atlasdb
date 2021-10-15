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
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.CommitUpdate.Visitor;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.immutables.value.Value;

final class FilteringValueCacheSnapshot implements ValueCacheSnapshot {
    private final ValueCacheSnapshot delegate;
    private final LockedCells lockedCells;

    private FilteringValueCacheSnapshot(ValueCacheSnapshot delegate, LockedCells lockedCells) {
        this.delegate = delegate;
        this.lockedCells = lockedCells;
    }

    static ValueCacheSnapshot create(ValueCacheSnapshot delegate, CommitUpdate commitUpdate) {
        return new FilteringValueCacheSnapshot(delegate, toLockedCells(commitUpdate));
    }

    @Override
    public Optional<CacheEntry> getValue(CellReference cellReference) {
        if (!lockedCells.isUnlocked(cellReference)) {
            return Optional.of(CacheEntry.locked());
        } else {
            return delegate.getValue(cellReference);
        }
    }

    @Override
    public boolean isUnlocked(CellReference cellReference) {
        return lockedCells.isUnlocked(cellReference) && delegate.isUnlocked(cellReference);
    }

    @Override
    public boolean isWatched(TableReference tableReference) {
        return delegate.isWatched(tableReference);
    }

    @Override
    public boolean hasAnyTablesWatched() {
        return delegate.hasAnyTablesWatched();
    }

    private static LockedCells toLockedCells(CommitUpdate commitUpdate) {
        return commitUpdate.accept(new Visitor<LockedCells>() {
            @Override
            public LockedCells invalidateAll() {
                return LockedCells.invalidateAll();
            }

            @Override
            public LockedCells invalidateSome(Set<LockDescriptor> invalidatedLocks) {
                return LockedCells.invalidateSome(invalidatedLocks);
            }
        });
    }

    @Value.Immutable
    interface LockedCells {
        boolean allLocked();

        Set<CellReference> lockedCells();

        static LockedCells invalidateAll() {
            return ImmutableLockedCells.builder().allLocked(true).build();
        }

        static LockedCells invalidateSome(Set<LockDescriptor> descriptors) {
            return ImmutableLockedCells.builder()
                    .allLocked(false)
                    .lockedCells(descriptors.stream()
                            .map(AtlasLockDescriptorUtils::candidateCells)
                            .flatMap(List::stream)
                            .collect(Collectors.toSet()))
                    .build();
        }

        default boolean isUnlocked(CellReference cellReference) {
            return !allLocked() && !lockedCells().contains(cellReference);
        }
    }
}
