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

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.SweepStrategy;

public final class SweepStrategyManagers {
    private SweepStrategyManagers() {}

    public static SweepStrategyManager create(TableMetadataManager tableMetadataManager) {
        // THIS IS A CHANGE IN BEHAVIOR - CHECK IF FINE
        return tableRef -> tableMetadataManager
                .get(tableRef)
                .map(tableMetadata -> SweepStrategy.from(tableMetadata.getSweepStrategy()))
                .orElseGet(() -> SweepStrategy.from(TableMetadataPersistence.SweepStrategy.CONSERVATIVE));
    }

    public static SweepStrategyManager createWithoutWarmingCache(KeyValueService keyValueService) {
        TableMetadataManager tableMetadataManager = TableMetadataManagers.createWithoutWarmingCache(keyValueService);
        return create(tableMetadataManager);
    }

    public static SweepStrategyManager completelyConservative() {
        return tableRef -> SweepStrategy.from(TableMetadataPersistence.SweepStrategy.CONSERVATIVE);
    }
}
