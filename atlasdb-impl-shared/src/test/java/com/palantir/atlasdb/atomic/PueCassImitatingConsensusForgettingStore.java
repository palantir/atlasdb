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

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.common.streams.KeyedStream;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

public class PueCassImitatingConsensusForgettingStore extends CassandraImitatingConsensusForgettingStore {

    public PueCassImitatingConsensusForgettingStore(double probabilityOfFailure) {
        super(probabilityOfFailure);
    }

    /**
     * Atomically performs a read (potentially propagating newest read value) and if there is no value present on any of
     * the nodes in a quorum, writes the value to those nodes.
     *
     * This operation is guarded by a write lock on cell to prevent the values on any of the quorum of nodes from being
     * changed between the read and the write.
     *
     * @return {@link AtomicOperationResult} with success if the atomic update was successful. Else,
     * {@link AtomicOperationResult} with {@link KeyAlreadyExistsException} with detail if there is a value
     * present against this key.
     */
    @Override
    public AtomicOperationResult atomicUpdate(Cell cell, byte[] value) {
        try {
            runAtomically(cell, () -> {
                Set<Node> quorumNodes = getQuorumNodes();
                Optional<BytesAndTimestamp> readResult = getInternal(cell, quorumNodes);
                if (readResult.isPresent()) {
                    throw new KeyAlreadyExistsException("The cell was not empty", ImmutableSet.of(cell));
                }
                writeToQuorum(cell, quorumNodes, value);
            });
        } catch (KeyAlreadyExistsException ex) {
            return AtomicOperationResult.failure(ex);
        }
        return AtomicOperationResult.success();
    }

    @Override
    public Map<Cell, AtomicOperationResult> atomicUpdate(Map<Cell, byte[]> values) {
        // sort by cells to avoid deadlock
        return KeyedStream.ofEntries(values.entrySet().stream().sorted(Map.Entry.comparingByKey()))
                .map((BiFunction<Cell, byte[], AtomicOperationResult>) this::atomicUpdate)
                .collectToMap();
    }
}
