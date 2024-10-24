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
package com.palantir.atlasdb.sweep.queue;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Adds {@link WriteInfo}s to a global queue to be swept.
 */
public interface MultiTableSweepQueueWriter {
    MultiTableSweepQueueWriter NO_OP = ignored -> {};

    default void enqueue(Map<TableReference, ? extends Map<Cell, byte[]>> writes, long timestamp) {
        enqueue(toWriteInfos(writes, timestamp));
    }

    /**
     * Persists the information about the writes into the sweep queue.
     *
     * @param writes list of writes to persist the information for
     */
    void enqueue(List<WriteInfo> writes);

    default List<WriteInfo> toWriteInfos(Map<TableReference, ? extends Map<Cell, byte[]>> writes, long timestamp) {
        return writes.entrySet().stream()
                .flatMap(entry -> entry.getValue().entrySet().stream()
                        .map(singleWrite -> SweepQueueUtils.toWriteInfo(entry.getKey(), singleWrite, timestamp)))
                .collect(Collectors.toList());
    }

    default SweeperStrategy getSweepStrategy(TableReference tableReference) {
        return SweeperStrategy.NON_SWEEPABLE;
    }
}
