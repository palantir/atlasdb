package com.palantir.atlas.impl;

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
