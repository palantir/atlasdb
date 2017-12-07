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

import java.util.function.Supplier;

import com.palantir.atlasdb.keyvalue.api.TableReference;

public class SweepQueueProcessor {

    private final SweepQueueReader reader;
    private final SweepDeleter deleter;
    private final Supplier<Long> sweepTimestamp;

    private final TableReference tableRef;

    public SweepQueueProcessor(SweepQueueReader reader, SweepDeleter deleter,
            Supplier<Long> sweepTimestamp, TableReference tableRef) {
        this.reader = reader;
        this.deleter = deleter;
        this.sweepTimestamp = sweepTimestamp;
        this.tableRef = tableRef;
    }

    public void runOneIteration() {
        WriteBatch batch = reader.getNext(tableRef, sweepTimestamp.get());

        deleter.delete(batch.writes());

        reader.truncateUpTo(tableRef, batch.lastOffset());
    }

}
