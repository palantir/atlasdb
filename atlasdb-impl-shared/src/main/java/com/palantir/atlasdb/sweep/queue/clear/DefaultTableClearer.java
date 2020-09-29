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

package com.palantir.atlasdb.sweep.queue.clear;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.sweep.queue.ImmutableTimestampSupplier;
import com.palantir.atlasdb.sweep.queue.TargetedSweepFilter;
import com.palantir.atlasdb.sweep.queue.WriteInfo;
import com.palantir.atlasdb.table.description.TableMetadata;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public final class DefaultTableClearer implements TableClearer, TargetedSweepFilter {
    private final LoadingCache<TableReference, SweepStrategy> sweepStrategies;
    private final KeyValueService kvs;
    private final ImmutableTimestampSupplier immutableTimestampSupplier;
    private final ConservativeSweepWatermarkStore watermarkStore;

    @VisibleForTesting
    DefaultTableClearer(
            ConservativeSweepWatermarkStore watermarkStore,
            KeyValueService kvs,
            ImmutableTimestampSupplier immutableTimestampSupplier) {
        this.watermarkStore = watermarkStore;
        sweepStrategies = Caffeine.newBuilder()
                .maximumSize(10_000)
                .build(table -> Optional.ofNullable(kvs.getMetadataForTable(table))
                        .filter(metadata -> metadata.length != 0)
                        .map(TableMetadata.BYTES_HYDRATOR::hydrateFromBytes)
                        .map(TableMetadata::getSweepStrategy)
                        .orElse(null));
        this.kvs = kvs;
        this.immutableTimestampSupplier = immutableTimestampSupplier;
    }

    public DefaultTableClearer(KeyValueService kvs, ImmutableTimestampSupplier immutableTimestampSupplier) {
        this(new DefaultConservativeSweepWatermarkStore(kvs), kvs, immutableTimestampSupplier);
    }

    @Override
    public Collection<WriteInfo> filter(Collection<WriteInfo> writeInfos) {
        Set<TableReference> tablesToCareAbout = getConservativeTables(
                writeInfos.stream().map(WriteInfo::tableRef).collect(Collectors.toSet()));

        if (tablesToCareAbout.isEmpty()) {
            return writeInfos;
        }

        Map<TableReference, Long> truncationTimes = watermarkStore.getWatermarks(tablesToCareAbout);
        return writeInfos.stream()
                .filter(write -> !truncationTimes.containsKey(write.tableRef())
                        || write.timestamp() > truncationTimes.get(write.tableRef()))
                .collect(Collectors.toList());
    }

    private Set<TableReference> getConservativeTables(Set<TableReference> tables) {
        return tables.stream()
                .filter(table -> Optional.ofNullable(sweepStrategies.get(table))
                        .map(SweepStrategy.CONSERVATIVE::equals)
                        .orElse(false))
                .collect(Collectors.toSet());
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

        watermarkStore.updateWatermarks(immutableTimestamp, conservativeTables);
    }
}
