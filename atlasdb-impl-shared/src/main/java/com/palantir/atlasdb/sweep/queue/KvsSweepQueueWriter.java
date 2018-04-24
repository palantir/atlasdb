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
import java.util.Map;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public abstract class KvsSweepQueueWriter implements SweepQueueWriter {
    private final KeyValueService kvs;
    private final TableReference tableRef;

    public KvsSweepQueueWriter(KeyValueService kvs, TableReference tableRef) {
        this.kvs = kvs;
        this.tableRef = tableRef;
    }

    @Override
    public void enqueue(List<WriteInfo> writes) {
        kvs.put(tableRef, batchWrites(writes), 0L);
    }

    protected abstract Map<Cell, byte[]> batchWrites(List<WriteInfo> writes);
}
