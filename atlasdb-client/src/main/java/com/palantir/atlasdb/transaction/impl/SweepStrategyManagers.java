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
package com.palantir.atlasdb.transaction.impl;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.SweepStrategy;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;

public class SweepStrategyManagers {
    private SweepStrategyManagers() {
        //
    }

    public static SweepStrategyManager createDefault(KeyValueService kvs) {
        // Wrap in a RecomputingSupplier for its logic to protect against concurrent initialization
        RecomputingSupplier<LoadingCache<TableReference, SweepStrategy>> sweepStrategySupplierLoadingCache =
                RecomputingSupplier.create(() -> {
                    // On a cache miss, load metadata only for the relevant table. Helpful when many dynamic tables.
                    LoadingCache<TableReference, SweepStrategy> cache = Caffeine.newBuilder()
                            .expireAfterAccess(1, TimeUnit.DAYS)
                            .build(tableRef -> getSweepStrategy(kvs.getMetadataForTable(tableRef)));

                    // On async initialization, add all tables to optimize for cases when using mostly non-dynamic tables.
                    cache.putAll(getSweepStrategies(kvs));

                    return cache;
                });

        return new SweepStrategyManager() {
            @Override
            public SweepStrategy get(TableReference tableRef) {
                return sweepStrategySupplierLoadingCache.get().get(tableRef);
            }

            @Override
            public void invalidateCaches(Set<TableReference> tableRefs) {
                sweepStrategySupplierLoadingCache.get().invalidateAll(tableRefs);
            }
        };
    }

    public static SweepStrategyManager createFromSchema(Schema schema) {
        return tableRef -> {
            TableMetadata tableMeta = Preconditions.checkNotNull(
                    schema.getAllTablesAndIndexMetadata().get(tableRef),
                    "unknown table",
                    SafeArg.of("tableRef", tableRef));
            return SweepStrategy.from(tableMeta.getSweepStrategy());
        };
    }

    public static SweepStrategyManager fromMap(final Map<TableReference, SweepStrategy> map) {
        return tableRef -> Preconditions.checkNotNull(
                map.get(tableRef),
                "unknown table",
                SafeArg.of("tableRef", tableRef));
    }

    public static SweepStrategyManager completelyConservative() {
        return tableRef -> SweepStrategy.from(TableMetadataPersistence.SweepStrategy.CONSERVATIVE);
    }

    private static Map<TableReference, SweepStrategy> getSweepStrategies(KeyValueService kvs) {
        return ImmutableMap.copyOf(Maps.transformValues(kvs.getMetadataForTables(),
                SweepStrategyManagers::getSweepStrategy));
    }

    private static SweepStrategy getSweepStrategy(byte[] tableMeta) {
        if (tableMeta != null && tableMeta.length > 0) {
            return SweepStrategy.from(TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(tableMeta).getSweepStrategy());
        } else {
            return SweepStrategy.from(TableMetadataPersistence.SweepStrategy.CONSERVATIVE);
        }
    }
}
