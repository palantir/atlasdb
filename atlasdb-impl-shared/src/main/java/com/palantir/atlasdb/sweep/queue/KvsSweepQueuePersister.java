/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.schema.TargetedSweepSchema;
import com.palantir.atlasdb.table.description.Schemas;

public class KvsSweepQueuePersister implements MultiTableSweepQueueWriter {
    private final KeyValueService kvs;
    private final SweepableCellsWriter sweepableCellsWriter;
    private final SweepableTimestampsWriter sweepableTimestampsWriter;

    public KvsSweepQueuePersister(KeyValueService kvs) {
        WriteInfoPartitioner partitioner =  new WriteInfoPartitioner(kvs);
        this.kvs = kvs;
        this.sweepableCellsWriter = new SweepableCellsWriter(kvs, partitioner);
        this.sweepableTimestampsWriter = new SweepableTimestampsWriter(kvs, partitioner);
        initialize();
    }

    public void initialize() {
        Schemas.createTablesAndIndexes(TargetedSweepSchema.INSTANCE.getLatestSchema(), kvs);
    }

    @Override
    public void enqueue(List<WriteInfo> writes) {
        sweepableCellsWriter.enqueue(writes);
        sweepableTimestampsWriter.enqueue(writes);
    }
}
