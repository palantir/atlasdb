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

import java.util.Map;
import java.util.stream.Collectors;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;

/**
 * Adds {@link WriteInfo}s to a global queue to be swept.
 */
public interface SweepQueueWriter {

    SweepQueueWriter NO_OP = (table, writes) -> { };

    default void enqueue(Map<TableReference, ? extends Map<Cell, byte[]>> writesByTable, long timestamp) {
        writesByTable.forEach((table, writes) -> enqueue(table, writes, timestamp));
    }

    default void enqueue(TableReference table, Map<Cell, byte[]> writes, long timestamp) {
        enqueue(table, writes.entrySet().stream()
                .map(entry -> ImmutableWriteInfo.builder()
                        .cell(entry.getKey())
                        .isTombstone(Value.isTombstone(entry.getValue()))
                        .timestamp(timestamp)
                        .build())
                .collect(Collectors.toList()));
    }

    void enqueue(TableReference table, Iterable<WriteInfo> writes);

}
