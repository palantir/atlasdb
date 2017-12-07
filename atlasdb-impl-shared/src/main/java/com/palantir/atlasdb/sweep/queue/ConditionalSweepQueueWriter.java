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

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;

public class ConditionalSweepQueueWriter implements SweepQueueWriter {

    private final SweepStrategyManager sweepStrategyManager;
    private final SweepQueueWriter delegate;

    public ConditionalSweepQueueWriter(
            SweepQueueWriter delegate,
            SweepStrategyManager sweepStrategyManager) {
        this.delegate = delegate;
        this.sweepStrategyManager = sweepStrategyManager;
    }

    @Override
    public void enqueue(TableReference table, Map<Cell, byte[]> writes, long timestamp) {
        if (sweepStrategyManager.useTargetedSweepForTable(table)) {
            delegate.enqueue(table, writes, timestamp);
        }
    }

    @Override
    public void enqueue(TableReference table, Iterable<Write> writes) {
        if (sweepStrategyManager.useTargetedSweepForTable(table)) {
            delegate.enqueue(table, writes);
        }
    }
}
