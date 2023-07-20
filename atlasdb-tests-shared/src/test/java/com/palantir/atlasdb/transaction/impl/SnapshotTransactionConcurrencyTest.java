/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import static com.palantir.atlasdb.transaction.impl.TransactionTestUtils.unwrapSnapshotTransaction;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.AtlasDbTestCase;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.ValueAndChangeMetadata;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.watch.ChangeMetadata;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class SnapshotTransactionConcurrencyTest extends AtlasDbTestCase {
    private static final TableReference TABLE = TableReference.create(Namespace.DEFAULT_NAMESPACE, "test-table");
    private static final ExecutorService EXECUTOR = PTExecutors.newCachedThreadPool();

    // We can save ourselves a separate concurrency test for deleteWithMetadata() since it just calls putInternal()
    // with the empty byte array value.
    @Test
    public void storingMetadataIsAtomicWithLocalWrite()
            throws InterruptedException, ExecutionException, TimeoutException {
        int numThreads = 20;
        int numCells = 50;
        long randomSeed = System.currentTimeMillis();

        Random random = new Random(randomSeed);
        SnapshotTransaction txn = unwrapSnapshotTransaction(txManager.createNewTransaction());
        Set<Cell> cells = IntStream.range(0, numCells)
                .mapToObj(cellIndex -> Cell.create(PtBytes.toBytes(cellIndex), PtBytes.toBytes(cellIndex)))
                .collect(Collectors.toSet());

        List<Map<Cell, ChangeMetadata>> metadataWrittenByThread =
                concurrentlyWriteMetadataToCells(random, txn, cells, numThreads).get(200, TimeUnit.MILLISECONDS);
        Map<Cell, ChangeMetadata> internalMetadata = txn.getChangeMetadataForWrites(TABLE);
        for (Cell cell : cells) {
            byte[] currInternalValue = txn.get(TABLE, ImmutableSet.of(cell)).get(cell);
            // Every thread writes their thread ID to the cell, so the value is always 4 bytes.
            // The winning thread is the last one to write, as indicated by the current value
            int winningThreadIndex = ByteBuffer.wrap(currInternalValue).getInt();

            ChangeMetadata expectedMetadata =
                    metadataWrittenByThread.get(winningThreadIndex).get(cell);
            ChangeMetadata actualMetadata = internalMetadata.get(cell);

            assertThat(actualMetadata)
                    .as(
                            "Writing a value to the local buffer should be atomic with writing its metadata. If a"
                                + " value was written without metadata, the metadata should be cleared. Random seed:"
                                + " %d",
                            randomSeed)
                    .isEqualTo(expectedMetadata);
        }
    }

    // TODO test for compare and set bug

    /**
     * Returns a future containing the metadata actually written by the different threads (0..numThreads-1)
     */
    private static ListenableFuture<List<Map<Cell, ChangeMetadata>>> concurrentlyWriteMetadataToCells(
            Random random, Transaction txn, Set<Cell> cells, int numThreads) {
        List<Callable<Void>> tasks = new ArrayList<>();
        List<Map<Cell, ChangeMetadata>> metadataWrittenByThread = IntStream.range(0, numThreads)
                .<Map<Cell, ChangeMetadata>>mapToObj(threadIndex -> {
                    Map<Cell, ValueAndChangeMetadata> valuesToPut =
                            generateValuesAndMetadataForThread(random, cells, threadIndex);
                    // Not writing metadata is equivalent to removing metadata for the affected cells
                    if (random.nextBoolean()) {
                        tasks.add(() -> {
                            txn.putWithMetadata(TABLE, valuesToPut);
                            return null;
                        });
                        return Maps.transformValues(valuesToPut, ValueAndChangeMetadata::metadata);
                    } else {
                        Map<Cell, byte[]> valuesOnly = Maps.transformValues(valuesToPut, ValueAndChangeMetadata::value);
                        tasks.add(() -> {
                            txn.put(TABLE, valuesOnly);
                            return null;
                        });
                        return ImmutableMap.of();
                    }
                })
                .collect(Collectors.toUnmodifiableList());
        ListeningExecutorService concurrentExecutor =
                MoreExecutors.listeningDecorator(PTExecutors.newFixedThreadPool(numThreads));
        List<ListenableFuture<Void>> futures =
                tasks.stream().map(concurrentExecutor::submit).collect(Collectors.toUnmodifiableList());
        return Futures.whenAllComplete(futures).call(() -> metadataWrittenByThread, EXECUTOR);
    }

    private static Map<Cell, ValueAndChangeMetadata> generateValuesAndMetadataForThread(
            Random random, Set<Cell> cells, int threadId) {
        byte[] valueToPut = ByteBuffer.allocate(4).putInt(threadId).array();
        ChangeMetadata metadataToPut = ChangeMetadata.created(valueToPut);
        return KeyedStream.of(cells.stream())
                .filter(_unused -> random.nextBoolean())
                .map(_unused -> ValueAndChangeMetadata.of(valueToPut, metadataToPut))
                .collectToMap();
    }
}
