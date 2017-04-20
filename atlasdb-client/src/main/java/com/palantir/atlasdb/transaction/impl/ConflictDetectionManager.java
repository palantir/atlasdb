/*
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
package com.palantir.atlasdb.transaction.impl;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public class ConflictDetectionManager {
    private static final Logger log = LoggerFactory.getLogger(ConflictDetectionManager.class);
    private final LoadingCache<TableReference, ConflictHandler> cache;

    /**
     *  This class does not make the mistake of attempting cache invalidation,
     *  so a table dropped by another instance may still be cached here.
     *
     *  This is okay in the case of a simple drop, but a same-name table drop
     *  and re-addition with a different Conflict Handler
     *  (where an external atlas instance handles both of these operations)
     *  will be incorrect. This is an unrealistic workflow
     *  and I'm fine with just documenting this behavior.
     *
     *  (This has always been the behavior of this class; I'm simply calling it out)
     */
    private ConflictDetectionManager(CacheLoader<TableReference, ConflictHandler> loader) {
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(100_000)
                .build(loader);
    }


    public static ConflictDetectionManager create(KeyValueService kvs) {
        ConflictDetectionManager conflictDetectionManager = new ConflictDetectionManager(
                new CacheLoader<TableReference, ConflictHandler>() {
                    @Override
                    public ConflictHandler load(TableReference tableReference) throws Exception {
                        byte[] metadata = kvs.getMetadataForTable(tableReference);
                        if (metadata == null) {
                            return ConflictHandler.RETRY_ON_WRITE_WRITE; // legacy behavior / this is dumb
                        }
                        return getConflictHandlerFromMetadata(metadata);
                    }
                });

        // kick off an async thread that attempts to fully warm this cache
        // if it fails (e.g. probably this user has way too many tables), that's okay,
        // we will be falling back on individually loading in tables as needed.
        new Thread(() -> {
            try {
                conflictDetectionManager.warmCacheWith(
                        Maps.transformValues(kvs.getMetadataForTables(),
                                ConflictDetectionManager::getConflictHandlerFromMetadata));
            } catch (Throwable t) {
                log.warn("There was a problem with pre-warming the conflict detection cache;"
                        + " if you have unusually high table scale, this might be expected."
                        + " Performance may be degraded until normal usage adequately warms the cache.", t);
            }
        }, "ConflictDetectionManager Cache Async Pre-Warm").start();

        return conflictDetectionManager;
    }

    public static ConflictDetectionManager createWithNoConflictDetection() {
        return new ConflictDetectionManager(
                new CacheLoader<TableReference, ConflictHandler>() {
                    @Override
                    public ConflictHandler load(TableReference tableReference) throws Exception {
                        return ConflictHandler.IGNORE_ALL;
                    }
                });
    }

    @VisibleForTesting
    public static ConflictDetectionManager createWithStaticConflictDetection(
            Map<TableReference, ConflictHandler> staticMap) {
        CacheLoader<TableReference, ConflictHandler> loader = new CacheLoader<TableReference, ConflictHandler>() {
            @Override
            public ConflictHandler load(TableReference tableReference) throws Exception {
                ConflictHandler conflictHandler = staticMap.get(tableReference);
                if (conflictHandler == null) {
                    return ConflictHandler.RETRY_ON_WRITE_WRITE; // legacy behavior / this is dumb
                }
                return conflictHandler;
            }
        };
        return new ConflictDetectionManager(
                loader);
    }

    private static ConflictHandler getConflictHandlerFromMetadata(byte[] metadata) {
        return TableMetadata.BYTES_HYDRATOR
                .hydrateFromBytes(metadata).getConflictHandler();
    }

    private void warmCacheWith(Map<TableReference, ConflictHandler> preload) {
        cache.putAll(preload);
    }

    @VisibleForTesting
    public Map<TableReference, ConflictHandler> getCachedValues() {
        return cache.asMap();
    }

    public ConflictHandler get(TableReference tableReference) {
        return cache.getUnchecked(tableReference);
    }
}
