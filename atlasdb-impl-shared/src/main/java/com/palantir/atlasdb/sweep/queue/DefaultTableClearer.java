/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.queue;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.schema.generated.TableClearsTable.TableClearsNamedColumn;
import com.palantir.atlasdb.schema.generated.TableClearsTable.TableClearsRow;
import com.palantir.atlasdb.schema.generated.TableClearsTable.TableClearsRowResult;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.streams.KeyedStream;

public final class DefaultTableClearer implements TableClearer, TargetedSweepFilter {
    private static final TableReference CLEARS = TargetedSweepTableFactory.of().getTableClearsTable(null).getTableRef();
    private final LoadingCache<TableReference, SweepStrategy> sweepStrategies;
    private final KeyValueService kvs;
    private final ImmutableTimestampSupplier immutableTimestampSupplier;

    public DefaultTableClearer(KeyValueService kvs, ImmutableTimestampSupplier immutableTimestampSupplier) {
        sweepStrategies = Caffeine.newBuilder()
                .maximumSize(10_000)
                .build(table -> Optional.ofNullable(kvs.getMetadataForTable(table))
                        .map(TableMetadata.BYTES_HYDRATOR::hydrateFromBytes)
                        .map(TableMetadata::getSweepStrategy)
                        .orElse(null));
        this.kvs = kvs;
        this.immutableTimestampSupplier = immutableTimestampSupplier;
    }

    @Override
    public Collection<WriteInfo> filter(Collection<WriteInfo> writeInfos) {
        Set<TableReference> tablesToCareAbout = getConservativeTables(
                writeInfos.stream().map(WriteInfo::tableRef).collect(Collectors.toSet()));

        if (tablesToCareAbout.isEmpty()) {
            return writeInfos;
        }

        Map<TableReference, Long> truncationTimes = getWatermarks(tablesToCareAbout);
        return writeInfos.stream()
                .filter(write -> !truncationTimes.containsKey(write.tableRef())
                        || write.timestamp() > truncationTimes.get(write.tableRef()))
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    Map<TableReference, Long> getWatermarks(Set<TableReference> tableReferences) {
        Set<Cell> cells = tableReferences.stream().map(DefaultTableClearer::cell).collect(Collectors.toSet());
        Map<Cell, Value> fetched = kvs.get(CLEARS, Maps.asMap(cells, ignored -> Long.MAX_VALUE));
        return KeyedStream.stream(fetched)
                .map((cell, value) -> RowResult.of(cell, value.getContents()))
                .map(TableClearsRowResult::of)
                .mapKeys((cell, rowResult) -> tableRef(rowResult.getRowName()))
                .map(TableClearsRowResult::getLastClearedTimestamp)
                .collectToMap();
    }

    private Set<TableReference> getConservativeTables(Set<TableReference> tables) {
        return tables.stream()
                .filter(table -> Optional.ofNullable(sweepStrategies.get(table))
                        .map(SweepStrategy.CONSERVATIVE::equals)
                        .orElse(false))
                .collect(Collectors.toSet());
    }

    private static Cell cell(TableReference table) {
        TableClearsRow row = TableClearsRow.of(table.getQualifiedName());
        return Cell.create(row.persistToBytes(), TableClearsNamedColumn.LAST_CLEARED_TIMESTAMP.getShortName());
    }

    private static TableReference tableRef(TableClearsRow row) {
        return TableReference.fromString(row.getTable());
    }

    @Override
    public void deleteAllRowsInTables(Set<TableReference> tables) {
        execute(tables, filtered -> filtered.forEach(table -> kvs.deleteRange(table, RangeRequest.all())));
    }

    @Override
    public void truncateTables(Set<TableReference> tables) {
        execute(tables, kvs::truncateTables);
    }

    @Override
    public void dropTables(Set<TableReference> tables) {
        execute(tables, kvs::dropTables);
    }

    private void execute(Set<TableReference> tables, Consumer<Set<TableReference>> destructiveAction) {
        Set<TableReference> conservativeTables = getConservativeTables(tables);
        Set<TableReference> safeTables = Sets.difference(tables, conservativeTables);

        destructiveAction.accept(safeTables);

        if (conservativeTables.isEmpty()) {
            return;
        }

        long immutableTimestamp = immutableTimestampSupplier.getImmutableTimestamp();

        destructiveAction.accept(conservativeTables);

        updateWatermarksForTables(immutableTimestamp, conservativeTables);
    }

    @VisibleForTesting
    void updateWatermarksForTables(long newWatermark, Set<TableReference> conservativeTables) {
        conservativeTables.forEach(table -> updateWatermarkForTable(newWatermark, table));
    }

    private void updateWatermarkForTable(long newWatermark, TableReference table) {
        while (true) {
            Cell cell = cell(table);
            Optional<Value> currentValue = Optional.ofNullable(
                    kvs.get(CLEARS, ImmutableMap.of(cell, Long.MAX_VALUE)).get(cell));
            Optional<Long> currentWatermark = currentValue.map(value -> RowResult.of(cell, value.getContents()))
                    .map(TableClearsRowResult::of)
                    .map(TableClearsRowResult::getLastClearedTimestamp);

            if (currentWatermark.isPresent() && currentWatermark.get() > newWatermark) {
                return;
            }

            CheckAndSetRequest request = currentWatermark
                    .map(watermark -> CheckAndSetRequest.singleCell(
                            CLEARS,
                            cell,
                            EncodingUtils.encodeVarLong(watermark),
                            EncodingUtils.encodeVarLong(newWatermark)))
                    .orElseGet(() -> CheckAndSetRequest.newCell(
                            CLEARS, cell, EncodingUtils.encodeVarLong(newWatermark)));

            try {
                kvs.checkAndSet(request);
                return;
            } catch (CheckAndSetException e) {
                // do nothing, we spin
            }
        }
    }
}
