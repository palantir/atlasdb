/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.impl;

import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.table.description.TableMetadata;

@Singleton
public class TableMetadataCache {
    private final LoadingCache<String, TableMetadata> cache;

    @Inject
    public TableMetadataCache(final KeyValueService kvs) {
        this.cache = CacheBuilder.newBuilder()
                .expireAfterAccess(15, TimeUnit.MINUTES)
                .build(new CacheLoader<String, TableMetadata>() {
            @Override
            public TableMetadata load(String tableName) throws Exception {
                byte[] rawMetadata = kvs.getMetadataForTable(tableName);
                return TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(rawMetadata);
            }
        });
    }

    public TableMetadata getMetadata(String tableName) {
        return cache.getUnchecked(tableName);
    }
}
