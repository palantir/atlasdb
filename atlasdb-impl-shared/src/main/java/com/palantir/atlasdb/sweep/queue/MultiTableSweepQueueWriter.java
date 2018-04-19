/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.queue;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;

/**
 * Adds {@link WriteInfo}s to a global queue to be swept.
 */
public interface MultiTableSweepQueueWriter {

    MultiTableSweepQueueWriter NO_OP = ignored -> { };

    default void enqueue(Map<TableReference, Map<Cell, byte[]>> writesByTable, long timestamp) {
        enqueue(toWriteInfos(writesByTable, timestamp));
    }

    void enqueue(List<WriteInfo> writes);

    default List<WriteInfo> toWriteInfos(Map<TableReference, Map<Cell, byte[]>> writesByTable, long timestamp) {
        return writesByTable.entrySet().stream()
                .flatMap(writesByTableEntry -> writesByTableEntry.getValue().keySet().stream()
                        .map(cell -> WriteInfo.of(writesByTableEntry.getKey(), cell, timestamp)))
                .collect(Collectors.toList());
    }
}
