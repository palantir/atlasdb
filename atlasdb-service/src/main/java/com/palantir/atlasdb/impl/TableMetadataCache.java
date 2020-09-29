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
package com.palantir.atlasdb.impl;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.TableMetadata;
import java.util.concurrent.TimeUnit;
import javax.annotation.CheckForNull;
import javax.inject.Inject;

public class TableMetadataCache {
    private final LoadingCache<String, TableMetadata> cache;
    private static final TableMetadata EMPTY = TableMetadata.allDefault();

    @Inject
    public TableMetadataCache(final KeyValueService kvs) {
        this.cache = CacheBuilder.newBuilder()
                .expireAfterAccess(15, TimeUnit.MINUTES)
                .build(new CacheLoader<String, TableMetadata>() {
                    @Override
                    public TableMetadata load(String tableName) throws Exception {
                        byte[] rawMetadata = kvs.getMetadataForTable(TableReference.createUnsafe(tableName));
                        if (rawMetadata == null || rawMetadata.length == 0) {
                            return EMPTY;
                        }
                        return TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(rawMetadata);
                    }
                });
    }

    @CheckForNull
    public TableMetadata getMetadata(String tableName) {
        TableMetadata ret = cache.getUnchecked(tableName);
        if (ret == EMPTY) {
            return null;
        }
        return ret;
    }
}
