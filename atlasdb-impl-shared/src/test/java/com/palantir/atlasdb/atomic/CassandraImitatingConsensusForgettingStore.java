/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.assertj.core.util.Streams;
import org.immutables.value.Value;

/**
 * This class simulates behaviour of Cassandra. In particular, the following:
 * 1. Whenever a read operation on a quorum of nodes encounters mismatched values, the latest (as determined by its
 *    timestamp) written value is determined to be the true value and the out of sync nodes are updated synchronously
 *    before returning.
 * 2. Only a single PuE/CaS can be executed for a given cell at any one time
 * 3. PuE/CaS are atomic: after the read that checks current values, it is guaranteed those values will not change until
 *    the write succeeds or the method throws.
 * 4. Any write operation, including the write from (1) can succeed on some number of nodes and then fail.
 * 5. We assume that replication equals the whole cluster. In practice there are issues around range movements but the
 *    consensus forgetting store doesn't handle that case.
 */
abstract class CassandraImitatingConsensusForgettingStore implements ConsensusForgettingStore {
    private static final int NUM_NODES = 5;
    private static final int QUORUM = NUM_NODES / 2 + 1;

    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private final AtomicLong timestamps = new AtomicLong(0);
    private final List<Node> nodes =
            IntStream.range(0, NUM_NODES).mapToObj(_ignore -> new Node()).collect(Collectors.toList());
    private final Map<Cell, ReentrantReadWriteLock> locks = new ConcurrentHashMap<>();

    private volatile double probabilityOfFailure;

    public CassandraImitatingConsensusForgettingStore(double probabilityOfFailure) {
        this.probabilityOfFailure = probabilityOfFailure;
    }

    /**
     * Atomically performs a read (potentially propagating newest read value) and if the latest value present on a
     * quorum of nodes matches the supplied value, writes the value back to those nodes (resetting the timestamp). If
     * the values do not match, throws a {@link CheckAndSetException} with detail.
     *
     * This operation is guarded by a write lock on cell to prevent the values on any of the quorum of nodes from being
     * changed between the read and the write.
     */
    @Override
    public void checkAndTouch(Cell cell, byte[] value) throws CheckAndSetException {
        runAtomically(cell, () -> {
            Set<Node> quorumNodes = getQuorumNodes();
            Optional<BytesAndTimestamp> readResult = getInternal(cell, quorumNodes);
            if (readResult.map(BytesAndTimestamp::bytes).stream().noneMatch(read -> Arrays.equals(read, value))) {
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
    public void checkAndTouch(Map<Cell, byte[]> values) throws CheckAndSetException {
        // sort by cells to avoid deadlock
        KeyedStream.ofEntries(values.entrySet().stream().sorted(Map.Entry.comparingByKey()))
                .forEach(this::checkAndTouch);
    }

    /**
     * Reads from a quorum of cells. If all values agree, returns the value; otherwise:
     *   1. Chooses the value with the greatest timestamp V
     *   2. Propagates V to all the nodes in the selected quorum, provided the values on those nodes are still older
     *   3. Returns V
     * Step (2) uses a read lock to avoid breaking the atomicity of the read and write of PuE and CaS. Step (2) can
     * fail after updating some or all of the nodes.
     */
    @Override
    public ListenableFuture<Optional<byte[]>> get(Cell cell) {
        try {
            return Futures.immediateFuture(getInternal(cell, getQuorumNodes()).map(BytesAndTimestamp::bytes));
        } catch (RuntimeException e) {
            return Futures.immediateFailedFuture(e);
        }
    }

    @Override
    public ListenableFuture<Map<Cell, byte[]>> getMultiple(Iterable<Cell> cells) {
        // sort by cells to avoid deadlock
        return AtlasFutures.allAsMap(
                KeyedStream.of(Streams.stream(cells).sorted()).map(this::get).collectToMap(),
                MoreExecutors.directExecutor());
    }

    /**
     * A simple put that may fail after updating some or all nodes. Uses a read lock to avoid breaking the atomicity of
     * PuE and CaS, but explicitly does not synchronise with other put and get operations.
     */
    @Override
    public void put(Cell cell, byte[] value) {
        runStateMutatingTaskOnNodes(
                cell,
                getQuorumNodes(),
                node -> node.put(cell, ImmutableBytesAndTimestamp.of(value, Long.MAX_VALUE - 10)));
    }

    @Override
    public void put(Map<Cell, byte[]> values) {
        // sort by cells to avoid deadlock
        KeyedStream.ofEntries(values.entrySet().stream().sorted(Map.Entry.comparingByKey()))
                .forEach(this::put);
    }

    public void setProbabilityOfFailure(double newProbability) {
        probabilityOfFailure = newProbability;
    }

    public Optional<BytesAndTimestamp> getInternal(Cell cell, Set<Node> quorumNodes) {
        Set<Optional<BytesAndTimestamp>> reads = quorumNodes.stream()
                .map(node -> Optional.ofNullable(node.get(cell)))
                .collect(Collectors.toSet());
        Optional<BytesAndTimestamp> result =
                reads.stream().flatMap(Optional::stream).max(Comparator.comparing(BytesAndTimestamp::timestamp));
        if (reads.size() > 1) {
            runStateMutatingTaskOnNodes(cell, quorumNodes, node -> node.tryUpdateTo(cell, result.get()));
        }
        return result;
    }

    private void maybeFail() {
        if (random.nextDouble() <= probabilityOfFailure) {
            throw new SafeRuntimeException("Ohno!");
        }
    }

    void writeToQuorum(Cell cell, Set<Node> quorumNodes, byte[] value) {
        BytesAndTimestamp tsValue = ImmutableBytesAndTimestamp.of(value, timestamps.getAndIncrement());
        runTaskOnNodesMaybeFail(quorumNodes, node -> {
            node.put(cell, tsValue);
        });
    }

    @SuppressWarnings("DangerousIdentityKey")
    Set<Node> getQuorumNodes() {
        return nodes.stream().collect(Collectors.collectingAndThen(Collectors.toCollection(ArrayList::new), list -> {
            Collections.shuffle(list);
            return ImmutableSet.copyOf(list.subList(0, QUORUM));
        }));
    }

    void runAtomically(Cell cell, Runnable task) {
        ReentrantReadWriteLock.WriteLock lock =
                getReentrantReadWriteLockForCell(cell).writeLock();
        lock.lock();
        try {
            task.run();
        } finally {
            lock.unlock();
        }
    }

    private void runStateMutatingTaskOnNodes(Cell cell, Set<Node> quorumNodes, Consumer<Node> task) {
        ReentrantReadWriteLock.ReadLock lock =
                getReentrantReadWriteLockForCell(cell).readLock();
        lock.lock();
        try {
            runTaskOnNodesMaybeFail(quorumNodes, task);
        } finally {
            lock.unlock();
        }
    }

    private void runTaskOnNodesMaybeFail(Set<Node> quorumNodes, Consumer<Node> task) {
        maybeFail();
        for (Node current : quorumNodes) {
            task.accept(current);
            maybeFail();
        }
    }

    private ReentrantReadWriteLock getReentrantReadWriteLockForCell(Cell cell) {
        return locks.computeIfAbsent(cell, _ignore -> new ReentrantReadWriteLock());
    }

    @Value.Immutable
    interface BytesAndTimestamp {
        @Value.Parameter
        byte[] bytes();

        @Value.Parameter
        long timestamp();

        default boolean isAfter(BytesAndTimestamp other) {
            if (other == null) {
                return true;
            }
            return Comparator.comparing(BytesAndTimestamp::timestamp).compare(this, other) > 0;
        }
    }

    static final class Node {
        private final Map<Cell, BytesAndTimestamp> data = new ConcurrentHashMap<>();

        private BytesAndTimestamp get(Cell cell) {
            return data.get(cell);
        }

        private void put(Cell cell, BytesAndTimestamp value) {
            data.put(cell, value);
        }

        private void tryUpdateTo(Cell cell, BytesAndTimestamp value) {
            data.compute(cell, (passedCell, oldValue) -> {
                if (value.isAfter(oldValue)) {
                    return value;
                }
                return oldValue;
            });
        }
    }
}
