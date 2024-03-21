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

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.table.description.SweepStrategy;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.atlasdb.transaction.api.OrphanedSentinelDeleter;
import com.palantir.common.streams.KeyedStream;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public final class DefaultOrphanedSentinelDeleter implements OrphanedSentinelDeleter {
    private final Function<TableReference, SweepStrategy> sweepStrategyProvider;
    private final DeleteExecutor deleteExecutor;

    public DefaultOrphanedSentinelDeleter(
            Function<TableReference, SweepStrategy> sweepStrategyProvider, DeleteExecutor deleteExecutor) {
        this.sweepStrategyProvider = sweepStrategyProvider;
        this.deleteExecutor = deleteExecutor;
    }

    @Override
    public void scheduleSentinelsForDeletion(TableReference tableReference, Set<Cell> orphanedSentinels) {
        if (orphanedSentinels.isEmpty()) {
            return;
        }
        if (sweepStrategyProvider
                .apply(tableReference)
                .getSweeperStrategy()
                .map(sweeperStrategy -> sweeperStrategy == SweeperStrategy.THOROUGH)
                .orElse(false)) {
            Map<Cell, Long> sentinels = KeyedStream.of(orphanedSentinels)
                    .map(_ignore -> Value.INVALID_VALUE_TIMESTAMP)
                    .collectToMap();
            deleteExecutor.scheduleForDeletion(tableReference, sentinels);
        }
    }
}
