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

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.SweepStrategy.SweeperStrategy;
import com.palantir.logsafe.SafeArg;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SweepQueueWriter implements MultiTableSweepQueueWriter {
    private static final Logger log = LoggerFactory.getLogger(SweepQueueWriter.class);

    private final SweepableTimestamps sweepableTimestamps;
    private final SweepableCells sweepableCells;
    private final WriteInfoPartitioner partitioner;

    SweepQueueWriter(
            SweepableTimestamps sweepableTimestamps, SweepableCells sweepableCells, WriteInfoPartitioner partitioner) {
        this.sweepableTimestamps = sweepableTimestamps;
        this.sweepableCells = sweepableCells;
        this.partitioner = partitioner;
    }

    @Override
    public void enqueue(List<WriteInfo> writes) {
        sweepableTimestamps.enqueue(writes);
        sweepableCells.enqueue(writes);
        log.debug("Enqueued {} writes into the sweep queue.", SafeArg.of("writes", writes.size()));
    }

    @Override
    public Optional<SweeperStrategy> getSweepStrategy(TableReference tableReference) {
        return partitioner.getStrategyForTable(tableReference);
    }
}
