/*
 * Copyright 2017 Palantir Technologies
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheLoader;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public final class ConflictDetectionManagers {
    private static final Logger log = LoggerFactory.getLogger(ConflictDetectionManagers.class);
    // back-compat with last impl. Having a silent default ConflictHandler is bad and we should fix this.
    public static final ConflictHandler DEFAULT_CONFLICT_HANDLER = ConflictHandler.RETRY_ON_WRITE_WRITE;

    private ConflictDetectionManagers() {}

    public static ConflictDetectionManager createWithNoConflictDetection() {
        return new ConflictDetectionManager(
                new CacheLoader<TableReference, ConflictHandler>() {
                    @Override
                    public ConflictHandler load(TableReference tableReference) throws Exception {
                        return ConflictHandler.IGNORE_ALL;
                    }
                });
    }

    public static ConflictDetectionManager create(KeyValueService kvs) {
        return create(kvs, true);
    }

    public static ConflictDetectionManager createWithoutWarmingCache(KeyValueService kvs) {
        return create(kvs, false);
    }

    private static ConflictDetectionManager create(KeyValueService kvs, boolean warmCache) {
        ConflictDetectionManager conflictDetectionManager = new ConflictDetectionManager(
                new CacheLoader<TableReference, ConflictHandler>() {
                    @Override
                    public ConflictHandler load(TableReference tableReference) throws Exception {
                        byte[] metadata = kvs.getMetadataForTable(tableReference);
                        if (metadata == null) {
                            log.error("Tried to make a transaction over a table that has no metadata: {}."
                                    + " Using the default conflict handler ({}).",
                                    tableReference, DEFAULT_CONFLICT_HANDLER.toString());
                            return DEFAULT_CONFLICT_HANDLER;
                        } else {
                            return getConflictHandlerFromMetadata(metadata);
                        }
                    }
                });
        if (warmCache) {
            // kick off an async thread that attempts to fully warm this cache
            // if it fails (e.g. probably this user has way too many tables), that's okay,
            // we will be falling back on individually loading in tables as needed.
            new Thread(() -> {
                try {
                    conflictDetectionManager.warmCacheWith(
                            Maps.transformValues(kvs.getMetadataForTables(),
                                    ConflictDetectionManagers::getConflictHandlerFromMetadata));
                } catch (Throwable t) {
                    log.warn("There was a problem with pre-warming the conflict detection cache;"
                            + " if you have unusually high table scale, this might be expected."
                            + " Performance may be degraded until normal usage adequately warms the cache.", t);
                }
            }, "ConflictDetectionManager Cache Async Pre-Warm").start();
        }
        return conflictDetectionManager;
    }

    private static ConflictHandler getConflictHandlerFromMetadata(byte[] metadata) {
        return TableMetadata.BYTES_HYDRATOR
                .hydrateFromBytes(metadata).getConflictHandler();
    }

}
