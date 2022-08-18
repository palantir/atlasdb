/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

public class UnsweepableCells extends SweepQueueTable {
    private final WriteInfoPartitioner partitioner;

    public UnsweepableCells(
            KeyValueService kvs,
            TableReference tableRef,
            WriteInfoPartitioner partitioner,
            @Nullable TargetedSweepMetrics metrics) {
        super(kvs, tableRef, partitioner, metrics);
        this.partitioner = partitioner;
    }

    @Override
    public void enqueue(List<WriteInfo> allWrites) {
        Map<PartitionInfo, List<WriteInfo>> partitionedUnsweepableWrites =
                partitioner.filterAndPartitionUnsweepableCells(allWrites);

        // todo(snadna): actually have a table to write to  and finish writing
    }


    @Override
    Map<Cell, byte[]> populateReferences(
            PartitionInfo partitionInfo, List<WriteInfo> writes) {
        return null;
    }

    @Override
    Map<Cell, byte[]> populateCells(PartitionInfo info, List<WriteInfo> writes) {
        return null;
    }
}
