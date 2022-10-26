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

package com.palantir.atlasdb.atomic;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.common.streams.KeyedStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public final class MarkAndCasCassImitatingConsensusForgettingStore extends CassandraImitatingConsensusForgettingStore {
    @VisibleForTesting
    static final byte[] IN_PROGRESS_MARKER = new byte[1];

    public MarkAndCasCassImitatingConsensusForgettingStore(double probabilityOfFailure) {
        super(probabilityOfFailure);
    }

    /**
     * Atomically performs a read (potentially propagating newest read value) and if the cell is marked on any of
     * the nodes in a quorum, writes the value to those nodes. If there is a value present, throws a
     * {@link CheckAndSetException} with detail.
     *
     * This operation is guarded by a write lock on cell to prevent the values on any of the quorum of nodes from being
     * changed between the read and the write.
     */
    @Override
    public void atomicUpdate(Cell cell, byte[] value) throws CheckAndSetException {
        runAtomically(cell, () -> {
            Set<Node> quorumNodes = getQuorumNodes();
            Optional<BytesAndTimestamp> readResult = getInternal(cell, quorumNodes);
            if (readResult
                    .map(BytesAndTimestamp::bytes)
                    .filter(read -> Arrays.equals(read, IN_PROGRESS_MARKER))
                    .isEmpty()) {
                throw new CheckAndSetException(
                        "Did not find the expected value",
                        cell,
                        value,
                        readResult.map(BytesAndTimestamp::bytes).stream().collect(Collectors.toList()));
            }
            writeToQuorum(cell, quorumNodes, value);
        });
    }

    @Override
    public Map<Cell, ListenableFuture<Void>> atomicUpdate(Map<Cell, byte[]> values) throws CheckAndSetException {
        // sort by cells to avoid deadlock
        KeyedStream.ofEntries(values.entrySet().stream().sorted(Map.Entry.comparingByKey()))
                .forEach(this::atomicUpdate);
    }

    @Override
    public void mark(Cell cell) {
        runAtomically(cell, () -> {
            Set<Node> quorumNodes = getQuorumNodes();
            writeToQuorum(cell, quorumNodes, IN_PROGRESS_MARKER);
        });
    }

    @Override
    public void mark(Set<Cell> cells) {
        // sort by cells to avoid deadlock
        cells.stream().sorted().forEach(this::mark);
    }
}
