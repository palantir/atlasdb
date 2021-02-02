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

import com.palantir.async.initializer.CallbackInitializable;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.SweepStrategy.SweeperStrategy;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Adds {@link WriteInfo}s to a global queue to be swept.
 */
public interface MultiTableSweepQueueWriter extends AutoCloseable, CallbackInitializable<TransactionManager> {
    MultiTableSweepQueueWriter NO_OP = ignored -> {};

    default void enqueue(Map<TableReference, ? extends Map<Cell, byte[]>> writes, long timestamp) {
        enqueue(toWriteInfos(writes, timestamp));
    }

    /**
     * Persists the information about the writes into the sweep queue.
     *
     * @param writes list of writes to persist the information for
     */
    void enqueue(List<WriteInfo> writes);

    /**
     * This method must be implemented if asynchronous initialization is necessary for the implementation. This is
     * generally the case if the transaction manager allows asynchronous initialization since there is no guarantee
     * the underlying kvs is ready at object creation time.
     *
     * @param txManager the transaction manager performing the callback
     */
    @Override
    default void initialize(TransactionManager txManager) {
        // noop
    }

    default List<WriteInfo> toWriteInfos(Map<TableReference, ? extends Map<Cell, byte[]>> writes, long timestamp) {
        return writes.entrySet().stream()
                .flatMap(entry -> entry.getValue().entrySet().stream()
                        .map(singleWrite -> SweepQueueUtils.toWriteInfo(entry.getKey(), singleWrite, timestamp)))
                .collect(Collectors.toList());
    }

    @Override
    default void close() {
        // noop
    }

    default Optional<SweeperStrategy> getSweepStrategy(TableReference tableReference) {
        return Optional.empty();
    }
}
