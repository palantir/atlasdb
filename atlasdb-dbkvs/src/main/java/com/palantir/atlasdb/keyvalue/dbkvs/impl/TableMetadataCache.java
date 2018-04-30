/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.TableMetadata;

public class TableMetadataCache {
    private static final TableMetadata EMPTY = new TableMetadata();

    private final Cache<TableReference, TableMetadata> cache = CacheBuilder.newBuilder()
            .maximumSize(10000)
            .expireAfterWrite(1, TimeUnit.HOURS)
            .build();
    private final DbTableFactory dbTables;

    public TableMetadataCache(DbTableFactory dbTables) {
        this.dbTables = dbTables;
    }

    @Nullable
    public TableMetadata getTableMetadata(TableReference tableRef, ConnectionSupplier conns) {
        TableMetadata metadataOrEmpty = getOrReturnEmpty(tableRef, conns);
        if (metadataOrEmpty == EMPTY) {
            return null;
        } else {
            return metadataOrEmpty;
        }
    }

    private TableMetadata getOrReturnEmpty(TableReference tableRef, ConnectionSupplier conns) {
        TableMetadata cached = cache.getIfPresent(tableRef);
        if (cached != null) {
            return cached;
        } else {
            byte[] rawMetadata = dbTables.createMetadata(tableRef, conns).getMetadata();
            TableMetadata hydrated = hydrateMetadata(rawMetadata);
            cache.put(tableRef, hydrated);
            return hydrated;
        }
    }

    private TableMetadata hydrateMetadata(byte[] rawMetadata) {
        if (rawMetadata == null || rawMetadata.length == 0) {
            return EMPTY;
        } else {
            return TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(rawMetadata);
        }
    }
}
