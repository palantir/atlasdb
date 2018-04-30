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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Consumer;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Queues;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.sweep.queue.SweepQueueReader;
import com.palantir.atlasdb.sweep.queue.SweepQueueWriter;
import com.palantir.atlasdb.sweep.queue.WriteInfo;

/**
 * In memory implementation of the sweep queue. Only intended to be used for testing.
 * TODO(nziebart): remove this once we have an implementation that uses the KVS.
 */
public final class InMemorySweepQueue implements SweepQueueReader, SweepQueueWriter {

    private static final Map<TableReference, InMemorySweepQueue> instancesByTable = Maps.newConcurrentMap();

    private Queue<WriteInfo> writes = Queues.newArrayDeque();

    public static InMemorySweepQueue instanceForTable(TableReference table) {
        return instancesByTable.computeIfAbsent(table, ignored -> new InMemorySweepQueue());
    }

    public static MultiTableSweepQueueWriter writer() {
        return writes -> instanceForTable(writes.get(0).writeRef().tableRef()).enqueue(writes);
    }

    private InMemorySweepQueue() {}

    public static void clear() {
        instancesByTable.clear();
    }

    public synchronized void consumeNextBatch(
            Consumer<Collection<WriteInfo>> consumer,
            long maxTimestampExclusive) {
        List<WriteInfo> batch = getNextBatch(maxTimestampExclusive);

        consumer.accept(batch);

        writes.removeAll(batch);
    }

    private List<WriteInfo> getNextBatch(long maxTimestampExclusive) {
        List<WriteInfo> batch = Lists.newArrayList();
        PeekingIterator<WriteInfo> iterator = Iterators.peekingIterator(writes.iterator());

        while (iterator.hasNext() && iterator.peek().timestamp() < maxTimestampExclusive) {
            batch.add(iterator.next());
        }
        return batch;
    }

    public synchronized void enqueue(List<WriteInfo> newWrites) {
        writes.addAll(newWrites);
    }

}
