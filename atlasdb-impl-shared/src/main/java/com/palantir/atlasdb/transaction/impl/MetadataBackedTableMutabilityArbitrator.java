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
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.Mutability;
import com.palantir.atlasdb.transaction.api.TableMutabilityArbitrator;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.time.Duration;

public final class MetadataBackedTableMutabilityArbitrator implements TableMutabilityArbitrator {
    static final SafeLogger log = SafeLoggerFactory.get(MetadataBackedTableMutabilityArbitrator.class);

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
        return fromProto(parsedMetadata.getMutability());
    }

    // TODO (jkong): This feels like a weird place for this
    private static Mutability fromProto(TableMetadataPersistence.Mutability mutability) {
        switch (mutability) {
            case MUTABLE:
                return Mutability.MUTABLE;
            case WEAK_IMMUTABLE:
                return Mutability.WEAK_IMMUTABLE;
            case STRONG_IMMUTABLE:
                return Mutability.STRONG_IMMUTABLE;
            default:
                log.warn(
                        "unexpected mutability type, returning MUTABLE because that is safe",
                        SafeArg.of("type", mutability),
                        new SafeRuntimeException("I exist to show you the stack trace."));
                return Mutability.MUTABLE;
        }
    }
}
