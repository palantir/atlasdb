/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.&#10;&#10;Licensed under the Apache License, Version 2.0 (the &quot;License&quot;);&#10;you may not use this file except in compliance with the License.&#10;You may obtain a copy of the License at&#10;&#10;    http://www.apache.org/licenses/LICENSE-2.0&#10;&#10;Unless required by applicable law or agreed to in writing, software&#10;distributed under the License is distributed on an &quot;AS IS&quot; BASIS,&#10;WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.&#10;See the License for the specific language governing permissions and&#10;limitations under the License.
 */

package com.palantir.atlasdb.transaction.impl;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

public class TableMetadataManagers implements TableMetadataManager {
    private static final SafeLogger log = SafeLoggerFactory.get(TableMetadataManagers.class);

    private final LoadingCache<TableReference, Optional<TableMetadata>> cache;

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
    private TableMetadataManagers(KeyValueService kvs) {
        this.cache = Caffeine.newBuilder()
                .expireAfterAccess(Duration.ofDays(1))
                .maximumSize(100_000)
                .build(tableReference -> {
                    byte[] metadata = kvs.getMetadataForTable(tableReference);
                    if (metadata == null || metadata.length == 0) {
                        log.error(
                                "Tried to make a transaction over a table that has no metadata: {}.",
                                LoggingArgs.tableRef("tableReference", tableReference));
                        return Optional.empty();
                    } else {
                        return Optional.of(TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadata));
                    }
                });
    }

    public static TableMetadataManagers createWithoutWarmingCache(KeyValueService kvs) {
        return new TableMetadataManagers(kvs);
    }

    public static TableMetadataManagers create(
            KeyValueService kvs, boolean warmCache, ExecutorService executorService) {
        TableMetadataManagers manager = new TableMetadataManagers(kvs);

        if (warmCache) {
            // kick off an async thread that attempts to fully warm this cache
            // if it fails (e.g. probably this user has way too many tables), that's okay,
            // we will be falling back on individually loading in tables as needed.
            executorService.submit(() -> {
                try {
                    manager.cache.putAll(Maps.transformValues(kvs.getMetadataForTables(), metadata -> {
                        if (metadata == null || metadata.length == 0) {
                            log.debug("Metadata was null for a table."
                                    + " Likely because the table is currently being created."
                                    + " Skipping warming cache for the table.");
                            return Optional.empty();
                        } else {
                            return Optional.of(TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadata));
                        }
                    }));
                } catch (Throwable t) {
                    log.warn(
                            "There was a problem with pre-warming the conflict detection cache; if you"
                                    + " have unusually high table scale, this might be expected."
                                    + " Performance may be degraded until normal usage adequately warms"
                                    + " the cache.",
                            t);
                }
            });
        }
        return manager;
    }

    public Optional<TableMetadata> get(TableReference tableRef) {
        return cache.get(tableRef);
    }

    @Override
    public Map<TableReference, Optional<TableMetadata>> asMap() {
        return cache.asMap();
    }
}
