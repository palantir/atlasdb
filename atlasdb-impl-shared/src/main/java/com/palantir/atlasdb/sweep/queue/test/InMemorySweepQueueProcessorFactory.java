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

package com.palantir.atlasdb.sweep.queue.test;

import java.util.Map;
import java.util.Optional;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.Sweeper;
import com.palantir.atlasdb.sweep.queue.SweepDeleter;
import com.palantir.atlasdb.sweep.queue.SweepDeleterImpl;
import com.palantir.atlasdb.sweep.queue.SweepQueueProcessor;
import com.palantir.atlasdb.sweep.queue.SweepTimestampProvider;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;

public class InMemorySweepQueueProcessorFactory {

    private final KeyValueService kvs;
    private final SweepStrategyManager sweepStrategyManager;
    private final SweepTimestampProvider sweepTimestamps;

    private final Map<TableReference, SweepQueueProcessor> processorsByTable = Maps.newConcurrentMap();

    public InMemorySweepQueueProcessorFactory(
            KeyValueService kvs,
            SweepStrategyManager sweepStrategyManager,
            SweepTimestampProvider sweepTimestamps) {
        this.kvs = kvs;
        this.sweepStrategyManager = sweepStrategyManager;
        this.sweepTimestamps = sweepTimestamps;
    }

    public SweepQueueProcessor getProcessorForTable(
            TableReference table) {
        return processorsByTable.computeIfAbsent(table, this::createProcessorForTable);
    }

    private SweepQueueProcessor createProcessorForTable(TableReference table) {
        InMemorySweepQueue queue = InMemorySweepQueue.instanceForTable(table);
        Optional<Sweeper> sweeper = Sweeper.of(sweepStrategyManager.get().get(table));
        Preconditions.checkState(sweeper.isPresent(), "Sweep strategy is NONE for table " + table);

        SweepDeleter deleter = new SweepDeleterImpl(table, kvs, sweeper.get());
        return new SweepQueueProcessor(() -> sweepTimestamps.getSweepTimestamp(sweeper.get()), queue, deleter);
    }

}
