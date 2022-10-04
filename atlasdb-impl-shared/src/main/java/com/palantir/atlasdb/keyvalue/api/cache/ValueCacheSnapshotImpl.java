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

import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.CellReference;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.RowReference;
import io.vavr.collection.Map;
import io.vavr.collection.Set;
import java.util.Optional;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@Value.Immutable
public interface ValueCacheSnapshotImpl extends ValueCacheSnapshot {
    Map<CellReference, CacheEntry> values();

    Set<TableReference> lockWatchEnabledTables();

    // The tables from these row refs may or may not be listed in lockWatchEnabledTables
    Set<RowReference> lockWatchEnabledRows();

    java.util.Set<TableReference> allowedTablesFromSchema();

    @Value.Derived
    default java.util.Set<TableReference> enabledTables() {
        return Sets.intersection(lockWatchEnabledTables().toJavaSet(), allowedTablesFromSchema());
    }

    @Value.Derived
    default java.util.Set<RowReference> enabledRows() {
        return lockWatchEnabledRows().toJavaSet().stream()
                .filter(rowReference -> allowedTablesFromSchema().contains(rowReference.tableRef()))
                .collect(Collectors.toSet());
    }

    @Override
    default Optional<CacheEntry> getValue(CellReference tableAndCell) {
        return values().get(tableAndCell).toJavaOptional();
    }

    @Override
    default boolean isUnlocked(CellReference tableAndCell) {
        return isWatched(tableAndCell.tableRef())
                && getValue(tableAndCell).map(CacheEntry::isUnlocked).orElse(true);
    }

    @Override
    default boolean isWatched(TableReference tableReference) {
        return enabledTables().contains(tableReference);
    }

    @Override
    default boolean isWatched(CellReference tableAndCell) {
        return enabledTables().contains(tableAndCell.tableRef())
                || enabledRows()
                        .contains(RowReference.of(
                                tableAndCell.tableRef(), tableAndCell.cell().getRowName()));
    }

    @Override
    default boolean hasAnyCellsWatched() {
        return !enabledTables().isEmpty() || !enabledRows().isEmpty();
    }

    static ValueCacheSnapshot of(
            Map<CellReference, CacheEntry> values,
            Set<TableReference> enabledTables,
            Set<RowReference> enabledRows,
            java.util.Set<TableReference> allowedTables) {
        return ImmutableValueCacheSnapshotImpl.builder()
                .values(values)
                .lockWatchEnabledTables(enabledTables)
                .lockWatchEnabledRows(enabledRows)
                .allowedTablesFromSchema(allowedTables)
                .build();
    }
}
