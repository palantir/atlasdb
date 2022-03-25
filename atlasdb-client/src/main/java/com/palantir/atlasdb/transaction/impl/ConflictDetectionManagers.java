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

import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import java.util.Map;
import java.util.Optional;

public final class ConflictDetectionManagers {
    private ConflictDetectionManagers() {}

    public static ConflictDetectionManager createWithNoConflictDetection() {
        return new ConflictDetectionManager() {
            @Override
            public Optional<ConflictHandler> get(TableReference _tableReference) {
                return Optional.of(ConflictHandler.IGNORE_ALL);
            }

            @Override
            public Map<TableReference, Optional<ConflictHandler>> asMap() {
                return Map.of();
            }
        };
    }

    public static ConflictDetectionManager createWithoutWarmingCache(KeyValueService kvs) {
        return new ConflictDetectionManagerImpl(TableMetadataManagers.createWithoutWarmingCache(kvs));
    }

    public static ConflictDetectionManager create(TableMetadataManager tableMetadataManager) {
        return new ConflictDetectionManagerImpl(tableMetadataManager);
    }

    private static final class ConflictDetectionManagerImpl implements ConflictDetectionManager {
        private final TableMetadataManager tableMetadataManager;

        private ConflictDetectionManagerImpl(TableMetadataManager tableMetadataManager) {
            this.tableMetadataManager = tableMetadataManager;
        }

        @Override
        public Optional<ConflictHandler> get(TableReference tableReference) {
            return tableMetadataManager.get(tableReference).map(TableMetadata::getConflictHandler);
        }

        @Override
        public Map<TableReference, Optional<ConflictHandler>> asMap() {
            return Maps.transformValues(
                    tableMetadataManager.asMap(),
                    tableMetadata -> tableMetadata.map(TableMetadata::getConflictHandler));
        }
    }
}
