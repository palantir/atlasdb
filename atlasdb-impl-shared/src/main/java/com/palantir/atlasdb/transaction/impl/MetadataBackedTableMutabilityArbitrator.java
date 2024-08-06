/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.Mutability;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.TableMutabilityArbitrator;
import java.time.Duration;

public final class MetadataBackedTableMutabilityArbitrator implements TableMutabilityArbitrator {
    // TODO (jkong): Is this really needed?
    private final RecomputingSupplier<LoadingCache<TableReference, Mutability>> kvsLoader;

    private MetadataBackedTableMutabilityArbitrator(
            RecomputingSupplier<LoadingCache<TableReference, Mutability>> kvsLoader) {
        this.kvsLoader = kvsLoader;
    }

    public static TableMutabilityArbitrator create(KeyValueService keyValueService) {
        RecomputingSupplier<LoadingCache<TableReference, Mutability>> kvsLoader = RecomputingSupplier.create(() -> {
            // On a cache miss, load metadata only for the relevant table. Helpful when many dynamic tables.
            LoadingCache<TableReference, Mutability> cache = Caffeine.newBuilder()
                    .expireAfterAccess(Duration.ofDays(1))
                    .build(tableRef -> parseMutability(keyValueService.getMetadataForTable(tableRef)));

            // Warm the cache.
            cache.putAll(Maps.transformValues(
                    keyValueService.getMetadataForTables(), MetadataBackedTableMutabilityArbitrator::parseMutability));

            return cache;
        });
        return new MetadataBackedTableMutabilityArbitrator(kvsLoader);
    }

    @Override
    public Mutability getMutability(TableReference tableReference) {
        return kvsLoader.get().get(tableReference);
    }

    private static Mutability parseMutability(byte[] tableMetadata) {
        TableMetadata parsedMetadata = TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(tableMetadata);
        return Mutability.fromProto(parsedMetadata.getMutability());
    }
}
